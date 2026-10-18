package node

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sst/sst/v3/pkg/runtime"
)

// The handler each invocation runs. It waits before it answers, as a real
// one does, since a request that follows a pause is the one that was held.
const handlerTakes = 200 * time.Millisecond

// What a call to the runtime API may add to an invocation, on the same
// machine. On Node.js 26.7, fetch held each one for about 300 ms.
const runtimeAPIBudget = 100 * time.Millisecond

type answer struct {
	path string
	body string
	took time.Duration
}

// runInvocations runs the real Node.js worker with the given handler, hands it
// invocations one after another, and returns how each was answered and how long
// after it was handed over.
func runInvocations(t *testing.T, code string, invocations int) []answer {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node isn't installed")
	}
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	dist := filepath.Join(repo, "platform", "dist", "nodejs-runtime", "index.js")
	if _, err := os.Stat(dist); err != nil {
		t.Skip("build the platform first: " + err.Error())
	}
	root := t.TempDir()
	platform := filepath.Join(root, ".sst", "platform", "dist", "nodejs-runtime")
	if err := os.MkdirAll(platform, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dist, filepath.Join(platform, "index.js")); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "out")
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "index.mjs"), []byte(code), 0644); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var handed time.Time
	served := 0
	answers := []answer{}
	done := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/invocation/next") {
			mu.Lock()
			if served == invocations {
				mu.Unlock()
				<-r.Context().Done()
				return
			}
			served++
			handed = time.Now()
			mu.Unlock()
			w.Header().Set("lambda-runtime-aws-request-id", "request")
			w.Header().Set("lambda-runtime-deadline-ms", "9999999999999")
			w.Write([]byte(`{"n":1}`))
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		answers = append(answers, answer{r.URL.Path, string(body), time.Since(handed)})
		if len(answers) == invocations {
			close(done)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(api.Close)

	worker, err := New("dev").Run(t.Context(), &runtime.RunInput{
		CfgPath:  filepath.Join(root, "sst.config.ts"),
		Server:   api.Listener.Addr().(*net.TCPAddr).String(),
		WorkerID: "test",
		Build:    &runtime.BuildOutput{Out: out, Handler: "index.handler"},
		Env:      []string{"PATH=" + os.Getenv("PATH")},
	})
	if err != nil {
		t.Fatal(err)
	}
	w := worker.(*Worker)
	t.Cleanup(func() { w.cmd.Process.Kill() })
	go io.Copy(io.Discard, w.Logs())

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("the worker answered %d of %d invocations", len(answers), invocations)
	}
	mu.Lock()
	defer mu.Unlock()
	return answers
}

// A worker answers its runtime API calls without holding them. Node.js 26.7's
// fetch held a request on a kept-alive connection until a timer fired, which
// added about 300 ms to each invocation here.
func TestWorkerAnswersWithoutDelay(t *testing.T) {
	cases := map[string]struct {
		code string
		path string
		body string
	}{
		"response": {
			code: `export const handler = async () => {
				await new Promise((r) => setTimeout(r, 200));
				return { statusCode: 200, body: "ok" };
			};`,
			path: "/2018-06-01/runtime/invocation/request/response",
			body: `{"statusCode":200,"body":"ok"}`,
		},
		"error": {
			code: `export const handler = async () => {
				await new Promise((r) => setTimeout(r, 200));
				throw new Error("it failed");
			};`,
			path: "/2018-06-01/runtime/invocation/request/error",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			for i, a := range runInvocations(t, c.code, 5) {
				if a.path != c.path {
					t.Errorf("invocation %d was answered at %s, want %s", i, a.path, c.path)
				}
				if c.body != "" && a.body != c.body {
					t.Errorf("invocation %d was answered with %s, want %s", i, a.body, c.body)
				}
				if c.body == "" {
					var e struct{ ErrorMessage string }
					if json.Unmarshal([]byte(a.body), &e); e.ErrorMessage != "it failed" {
						t.Errorf("invocation %d's error was %s", i, a.body)
					}
				}
				if extra := a.took - handlerTakes; extra > runtimeAPIBudget {
					t.Errorf("invocation %d was answered %v after its handler finished, want under %v", i, extra.Round(time.Millisecond), runtimeAPIBudget)
				}
			}
		})
	}
}

// A handler that fails to load is reported as an init error.
func TestWorkerReportsAnInitError(t *testing.T) {
	answers := runInvocations(t, `throw new Error("it failed to load");`, 1)
	if want := "/2018-06-01/runtime/init/error"; answers[0].path != want {
		t.Fatalf("the worker reported to %s, want %s", answers[0].path, want)
	}
	var e struct{ ErrorMessage string }
	if json.Unmarshal([]byte(answers[0].body), &e); e.ErrorMessage != "it failed to load" {
		t.Errorf("the init error was %s", answers[0].body)
	}
}

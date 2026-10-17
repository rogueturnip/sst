package aws

import (
	"sync"
	"testing"
	"time"

	"github.com/sst/sst/v3/cmd/sst/mosaic/watcher"
	"github.com/sst/sst/v3/pkg/bus"
)

// Saving a file rebuilds the functions that use it, and stops their workers,
// one that's running an invocation included. The invocation is never answered,
// and its bridge, which has had the ping, waits until Lambda times it out.
func TestReloadDoesNotDropARunningInvocation(t *testing.T) {
	running := make(chan struct{}, 1)
	release := make(chan struct{})
	h := newHarness(t, &fakeRuntime{rebuild: true, handle: func(event string) string {
		if event == "slow" {
			running <- struct{}{}
			<-release
		}
		return "echo " + event
	}})
	h.addFunction("Fn")
	b := h.bridge("Fn")
	if _, err := b.invoke("warm"); err != nil {
		t.Fatal(err)
	}

	reply := make(chan error, 1)
	go func() {
		_, err := b.invoke("slow")
		reply <- err
	}()
	<-running
	bus.Publish(&watcher.FileChangedEvent{Path: "src/handler.ts"})
	time.Sleep(200 * time.Millisecond)
	close(release)

	if err := <-reply; err == errNoReply {
		t.Fatal("the invocation running when the function was rebuilt was never answered")
	}
}

// A rebuild starts a new worker for every environment of the function at once,
// the idle ones too, so each save starts as many processes as there are
// environments, about 60 MB each for Node.js.
func TestReloadStartsWorkersOnlyForWhatIsInvoked(t *testing.T) {
	h := newHarness(t, &fakeRuntime{rebuild: true, handle: echo})
	h.addFunction("Fn")
	bridges := make([]*fakeBridge, 20)
	var wg sync.WaitGroup
	for i := range bridges {
		bridges[i] = h.bridge("Fn")
		wg.Add(1)
		go func(b *fakeBridge) {
			defer wg.Done()
			if _, err := b.invoke("warm"); err != nil {
				t.Error(err)
			}
		}(bridges[i])
	}
	wg.Wait()
	before := h.runtime.starts.Load()

	bus.Publish(&watcher.FileChangedEvent{Path: "src/handler.ts"})
	time.Sleep(500 * time.Millisecond)
	if _, err := bridges[0].invoke("after"); err != nil {
		t.Fatal(err)
	}
	if started := h.runtime.starts.Load() - before; started > 1 {
		t.Fatalf("one save started %d workers for %d idle environments, when one was invoked after", started, len(bridges))
	}
}

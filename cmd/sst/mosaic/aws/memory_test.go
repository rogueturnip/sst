package aws

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Lambda keeps making new execution environments for a function, and removes
// idle ones, so over a long session sst dev sees thousands. It keeps what it
// knows about each, its environment variables among them, after the worker has
// exited and the environment is gone, so its memory grows with every one.
func TestFunctionForgetsEnvironmentsThatAreGone(t *testing.T) {
	h := newHarness(t, &fakeRuntime{handle: echo, idle: 20 * time.Millisecond})
	h.addFunction("Fn")
	// a Lambda environment's variables, as a bridge sends them, are a few KB
	env := []string{"SST_RESOURCE_App=" + strings.Repeat("x", 10<<10)}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range 1000 {
		b := h.bridgeWithEnv("Fn", env)
		if _, err := b.invoke(fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("environment %d: %v", i, err)
		}
		b.close()
	}
	// the workers exit on idle, and the replies stop being sent again
	time.Sleep(replyResends*replyResendEvery + time.Second)
	runtime.GC()
	runtime.ReadMemStats(&after)
	if grew, limit := int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(3<<20); grew > limit {
		t.Fatalf("1,000 environments that are gone hold %d MB, want under %d MB", grew>>20, limit>>20)
	}
}

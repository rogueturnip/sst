package aws

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sst/sst/v3/cmd/sst/mosaic/aws/bridge"
)

// countReplies counts the replies published to a bridge, by ID.
func countReplies(h *harness, b *fakeBridge) func(id string) int {
	sub, _ := h.appsync.Subscribe(h.ctx, h.prefix+"/"+b.workerID+"/in")
	var mu sync.Mutex
	counts := map[string]int{}
	go func() {
		for {
			select {
			case <-h.ctx.Done():
				return
			case data := <-sub:
				var p bridge.Packet
				json.Unmarshal([]byte(data), &p)
				if p.Type == bridge.MessageResponse && p.Index == 0 {
					mu.Lock()
					counts[p.ID]++
					mu.Unlock()
				}
			}
		}
	}()
	return func(id string) int {
		mu.Lock()
		defer mu.Unlock()
		return counts[id]
	}
}

// A bridge only sends its next invocation once it's done with the one before,
// so a reply isn't sent again after that. A reply nothing follows still is, in
// case AppSync dropped it.
func TestReplyIsNotResentOnceTheBridgeMovesOn(t *testing.T) {
	h := newHarness(t, &fakeRuntime{handle: echo})
	h.addFunction("fn")
	b := h.bridge("fn")
	sent := countReplies(h, b)
	for i := range 3 {
		if _, err := b.invoke("event"); err != nil {
			t.Fatalf("invocation %d: %v", i, err)
		}
	}
	time.Sleep(replyResendEvery + 500*time.Millisecond)

	for n := 1; n <= 2; n++ {
		if times := sent(fmt.Sprintf("%s-%d", b.workerID, n)); times != 1 {
			t.Errorf("the reply to invocation %d was sent %d times, want once", n, times)
		}
	}
	if times := sent(fmt.Sprintf("%s-3", b.workerID)); times < 2 {
		t.Errorf("the reply to the last invocation was sent %d times, want it sent again", times)
	}
}

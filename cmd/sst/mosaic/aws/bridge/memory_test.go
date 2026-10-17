package bridge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// heapAfter runs f, and returns how much the live heap grew while it ran.
func heapAfter(f func()) int64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.GC()
	runtime.ReadMemStats(&after)
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

// sorted remembers where every message it has seen is up to, so that it can
// drop the copies the CLI sends of a reply. It never forgets one, so the
// memory grows with every message for as long as sst dev runs.
func TestSortedForgetsFinishedMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := make(chan string, 1024)
	seen := make(chan string, 1024)
	go func() {
		for p := range sorted(ctx, sub) {
			select {
			case seen <- p.ID:
			default:
			}
		}
	}()
	drain := func(id string) {
		for got := range seen {
			if got == id {
				return
			}
		}
	}
	grew := heapAfter(func() {
		for i := range 200_000 {
			sub <- packet(fmt.Sprintf("01JABCDEFGHJKMNPQRSTV%07d", i), 0, "{}", true)
		}
		sub <- packet("end", 0, "", true)
		drain("end")
	})
	if limit := int64(4 << 20); grew > limit {
		t.Fatalf("after 200,000 finished messages the heap grew by %d MB, want under %d MB", grew>>20, limit>>20)
	}
}

// A message that lost a packet is kept until it's whole, with the data of every
// packet that did arrive. One that never becomes whole is kept for good.
func TestSortedDropsMessagesThatNeverFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := make(chan string, 1024)
	seen := make(chan string, 16)
	go func() {
		for p := range sorted(ctx, sub) {
			seen <- p.ID
		}
	}()
	data := strings.Repeat("x", 64<<10)
	grew := heapAfter(func() {
		for i := range 500 {
			// packet 0 was lost, so packet 1 waits for it
			b, _ := json.Marshal(Packet{Type: MessageResponse, ID: fmt.Sprintf("lost-%d", i), Index: 1, Data: base64.StdEncoding.EncodeToString([]byte(data)), Final: true})
			sub <- string(b)
		}
		sub <- packet("end", 0, "", true)
		<-seen
	})
	if limit := int64(10 << 20); grew > limit {
		t.Fatalf("500 messages that never finished hold %d MB, want under %d MB", grew>>20, limit>>20)
	}
}

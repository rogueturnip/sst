package tcellterm

import (
	"fmt"
	"runtime"
	"testing"
)

// A pane in sst dev's multiplexer keeps every line that scrolls off its screen.
// Each line is a row of cells, about 100 bytes a cell, so a few hundred thousand
// lines of function output in a long session hold gigabytes. The scrollback has
// to be capped.
func TestScrollbackIsCapped(t *testing.T) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	vt := New()
	vt.Resize(200, 40)
	for i := range 50_000 {
		for _, r := range fmt.Sprintf("invocation %d printed a line", i) {
			vt.print(r)
		}
		vt.c0('\r')
		vt.c0('\n')
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if got, limit := len(vt.primaryScrollback), 10_000; got > limit {
		t.Fatalf("scrollback holds %d lines (%d MB) after 50,000, want at most %d", got, held>>20, limit)
	}
}

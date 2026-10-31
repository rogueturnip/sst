package server

import "testing"

func TestFlociDiscoverySeparatesTargets(t *testing.T) {
	cfg := "/app/sst.config.ts"
	cloud := resolveServerFile(cfg, "local")
	first := resolveFlociServerFile(cfg, "local", "http://127.0.0.1:4566")
	second := resolveFlociServerFile(cfg, "local", "http://127.0.0.1:4567")
	if cloud == first || cloud == second || first == second {
		t.Fatal("dev server discovery paths collide")
	}
	if first != resolveFlociServerFile(cfg, "local", "http://127.0.0.1:4566") {
		t.Fatal("discovery path unstable")
	}
}

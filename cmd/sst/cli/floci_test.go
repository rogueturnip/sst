package cli

import "testing"

func TestFlociCommandGuard(t *testing.T) {
	for _, command := range []string{"deploy", "diff", "refresh", "secret", "state"} {
		if err := flociCommandAllowed(command, false, true); err == nil {
			t.Errorf("allowed %s against Floci", command)
		}
		if err := flociCommandAllowed(command, true, false); err == nil {
			t.Errorf("allowed --floci for %s", command)
		}
		if err := flociCommandAllowed(command, false, false); err != nil {
			t.Errorf("cloud %s: %v", command, err)
		}
	}
	for _, command := range []string{"dev", "shell", "remove", "floci", "ui"} {
		if err := flociCommandAllowed(command, false, true); err != nil {
			t.Errorf("blocked local %s: %v", command, err)
		}
	}
}

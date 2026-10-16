package aws

import "fmt"

// maxOutputLine is the longest line of a function's or a task's output that's
// passed on, 1 MB, as for Run. A longer one is left out, with a note.
const maxOutputLine = 1024 * 1024

func tooLongNote(of string) string {
	return fmt.Sprintf("A line of the %s's output is longer than %d MB and is not shown.", of, maxOutputLine/(1024*1024))
}

package cmd

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// BuildCommit is the commit the binary was built from, set at link time:
//
//	go build -ldflags "-X github.com/musix/backhaul/cmd.BuildCommit=<sha>"
//
// When it is not, the version control stamp Go puts in a binary built from a
// checkout is used.
var BuildCommit string

// buildDescription names the build in a line of the log: logs of different
// builds of one deployment are otherwise hard to tell apart.
func buildDescription() string {
	commit, dirty := BuildCommit, ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if commit == "" {
					commit = s.Value
				}
			case "vcs.modified":
				if s.Value == "true" {
					dirty = "+modified"
				}
			}
		}
	}
	if commit == "" {
		commit = "unknown"
	}
	if len(commit) > 12 {
		commit = commit[:12]
	}
	return fmt.Sprintf("build %s%s, %s", commit, dirty, runtime.Version())
}

package main

type remoteOwnedProcess struct {
	PID        int    `json:"pid"`
	Identity   string `json:"identity"`
	Executable string `json:"executable"`
	Job        string `json:"job,omitempty"`
}

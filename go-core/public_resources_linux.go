package main

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func publicOSResources() (map[string]uint64, map[string]any) {
	var disk syscall.Statfs_t
	var capacity []uint64
	if syscall.Statfs(".", &disk) == nil && disk.Bsize > 0 && disk.Bavail <= disk.Blocks {
		capacity = []uint64{disk.Blocks * uint64(disk.Bsize), (disk.Blocks - disk.Bavail) * uint64(disk.Bsize)}
	}
	return readPublicOSResources(os.ReadFile, func(device string) bool {
		_, err := os.Stat("/sys/class/block/" + device + "/device")
		return err == nil
	}, capacity)
}

func readPublicOSResources(read func(string) ([]byte, error), physical func(string) bool, capacity []uint64) (map[string]uint64, map[string]any) {
	counters, values := map[string]uint64{}, map[string]any{}
	lines := func(name string) []string {
		raw, err := read(name)
		if err != nil {
			return nil
		}
		return strings.Split(string(raw), "\n")
	}
	number := func(raw string) (uint64, bool) {
		value, err := strconv.ParseUint(raw, 10, 64)
		return value, err == nil
	}
	if rows := lines("/proc/stat"); len(rows) > 0 {
		fields := strings.Fields(rows[0])
		if len(fields) >= 5 && fields[0] == "cpu" {
			var total, idle uint64
			valid := true
			for i := 1; i < len(fields) && i <= 8; i++ {
				n, ok := number(fields[i])
				valid = valid && ok
				total += n
				if i == 4 || i == 5 {
					idle += n
				}
			}
			if valid && total > 0 && idle <= total {
				counters["cpu"], counters["idle"] = total, idle
			}
		}
	}
	memory := map[string]uint64{}
	for _, line := range lines("/proc/meminfo") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[2] == "kB" {
			if n, ok := number(fields[1]); ok {
				memory[strings.TrimSuffix(fields[0], ":")] = n * 1024
			}
		}
	}
	if total, ok := memory["MemTotal"]; ok && total > 0 {
		values["memoryTotalBytes"] = total
		available, found := memory["MemAvailable"]
		if !found {
			available, found = memory["MemFree"]
		}
		if found && available <= total {
			values["memoryUsedBytes"] = total - available
		}
	}
	if len(capacity) == 2 && capacity[1] <= capacity[0] {
		values["diskTotalBytes"], values["diskUsedBytes"] = capacity[0], capacity[1]
	}
	var rx, tx uint64
	networkFound := false
	for _, line := range lines("/proc/net/dev") {
		pair := strings.SplitN(strings.TrimSpace(line), ":", 2)
		if len(pair) != 2 || pair[0] == "lo" || strings.HasPrefix(pair[0], "veth") || strings.HasPrefix(pair[0], "br-") || strings.HasPrefix(pair[0], "docker") {
			continue
		}
		fields := strings.Fields(pair[1])
		if len(fields) < 9 {
			continue
		}
		received, okRx := number(fields[0])
		sent, okTx := number(fields[8])
		if !okRx || !okTx {
			continue
		}
		networkFound = true
		rx += received
		tx += sent
	}
	if networkFound {
		counters["rx"], counters["tx"] = rx, tx
	}
	var reads, writes uint64
	diskFound := false
	for _, line := range lines("/proc/diskstats") {
		fields := strings.Fields(line)
		if len(fields) < 10 || !physical(fields[2]) {
			continue
		}
		readSectors, okRead := number(fields[5])
		writeSectors, okWrite := number(fields[9])
		if !okRead || !okWrite {
			continue
		}
		diskFound = true
		reads += readSectors * 512
		writes += writeSectors * 512
	}
	if diskFound {
		counters["read"], counters["write"] = reads, writes
	}
	return counters, values
}

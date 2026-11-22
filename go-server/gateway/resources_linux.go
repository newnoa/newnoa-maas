package main

import (
	"os"
	"strings"
	"syscall"
)

func osResources() (map[string]uint64, map[string]any) {
	counters := map[string]uint64{}
	values := map[string]any{}
	if raw, err := os.ReadFile("/proc/stat"); err == nil {
		fields := strings.Fields(strings.SplitN(string(raw), "\n", 2)[0])
		if len(fields) >= 5 {
			var total uint64
			for i := 1; i < len(fields) && i <= 8; i++ {
				total += number(fields[i])
			}
			idle := number(fields[4])
			if len(fields) > 5 {
				idle += number(fields[5])
			}
			counters["cpuTotal"] = total
			counters["cpuIdle"] = idle
		}
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		mem := map[string]uint64{}
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				mem[strings.TrimSuffix(f[0], ":")] = number(f[1]) * 1024
			}
		}
		if total, ok := mem["MemTotal"]; ok {
			values["memoryTotalBytes"] = total
			free := mem["MemAvailable"]
			if free == 0 {
				free = mem["MemFree"]
			}
			if free <= total {
				values["memoryUsedBytes"] = total - free
			}
		}
	}
	var fs syscall.Statfs_t
	if syscall.Statfs(".", &fs) == nil {
		values["diskTotalBytes"] = fs.Blocks * uint64(fs.Bsize)
		values["diskUsedBytes"] = (fs.Blocks - fs.Bavail) * uint64(fs.Bsize)
	}
	if raw, err := os.ReadFile("/proc/net/dev"); err == nil {
		var rx, tx uint64
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
			if len(parts) != 2 {
				continue
			}
			name := parts[0]
			if name == "lo" || strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "br-") || strings.HasPrefix(name, "docker") {
				continue
			}
			f := strings.Fields(parts[1])
			if len(f) >= 9 {
				rx += number(f[0])
				tx += number(f[8])
			}
		}
		counters["networkReceiveBytes"] = rx
		counters["networkTransmitBytes"] = tx
	}
	if raw, err := os.ReadFile("/proc/diskstats"); err == nil {
		var read, write uint64
		found := false
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) < 10 {
				continue
			}
			if _, err := os.Stat("/sys/class/block/" + f[2] + "/device"); err != nil {
				continue
			}
			found = true
			read += number(f[5]) * 512
			write += number(f[9]) * 512
		}
		if found {
			counters["diskReadBytes"] = read
			counters["diskWriteBytes"] = write
		}
	}
	return counters, values
}

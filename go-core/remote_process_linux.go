package main

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type remoteLinuxProcess struct {
	pid, group                    int
	uid, birth, state, executable string
}

func readRemoteLinuxProcess(pid int) (remoteLinuxProcess, error) {
	p := remoteLinuxProcess{pid: pid}
	if pid <= 1 {
		return p, errors.New("Invalid owned process identity")
	}
	root := fmt.Sprintf("/proc/%d/", pid)
	status, err := os.ReadFile(root + "status")
	if err != nil {
		return p, err
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "Uid:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				p.uid = fields[1]
			}
		}
	}
	raw, err := os.ReadFile(root + "stat")
	if err != nil {
		return p, err
	}
	end := strings.LastIndex(string(raw), ")")
	if end < 0 {
		return p, errors.New("Invalid process birth record")
	}
	fields := strings.Fields(string(raw)[end+1:])
	if len(fields) < 20 {
		return p, errors.New("Process birth record is incomplete")
	}
	p.state, p.birth = fields[0], fields[19]
	p.group, err = strconv.Atoi(fields[2])
	if err != nil {
		return p, errors.New("Invalid process group")
	}
	p.executable, _ = os.Readlink(root + "exe")
	return p, nil
}
func remoteProcessIdentity(pid int) (string, error) {
	p, err := readRemoteLinuxProcess(pid)
	if err != nil {
		return "", err
	}
	current, err := user.Current()
	if err != nil {
		return "", err
	}
	if p.uid == "" || p.uid != current.Uid {
		return "", errors.New("Owned process belongs to another operating system user")
	}
	if p.executable == "" {
		return "", errors.New("Owned executable identity is unavailable")
	}
	return p.uid + ":" + p.birth + ":" + p.executable, nil
}
func remoteLinuxGroup(group int) ([]remoteLinuxProcess, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	out := []remoteLinuxProcess{}
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || pid <= 1 {
			continue
		}
		p, e := readRemoteLinuxProcess(pid)
		if e != nil {
			if os.IsNotExist(e) {
				continue
			}
			return nil, errors.New("Process group membership could not be inspected securely")
		}
		if p.group == group && p.state != "Z" && p.state != "X" {
			out = append(out, p)
		}
	}
	return out, nil
}
func remoteOwnedIdentityParts(p remoteOwnedProcess) (string, uint64, error) {
	parts := strings.SplitN(p.Identity, ":", 3)
	if p.PID <= 1 || len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return "", 0, errors.New("Owned process identity cannot be verified")
	}
	birth, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, errors.New("Owned process birth identity cannot be verified")
	}
	current, err := user.Current()
	if err != nil || current.Uid != parts[0] {
		return "", 0, errors.New("Owned process belongs to another operating system user")
	}
	return parts[0], birth, nil
}
func remoteVerifyGroupLeader(p remoteOwnedProcess) (bool, error) {
	leader, err := readRemoteLinuxProcess(p.PID)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if leader.state == "Z" || leader.state == "X" {
		return false, nil
	}
	if leader.uid+":"+leader.birth+":"+leader.executable != p.Identity || leader.group != p.PID {
		return false, errors.New("Owned process birth identity changed; refusing process cleanup")
	}
	return true, nil
}
func remoteOwnedProcessAlive(p remoteOwnedProcess) (bool, error) {
	if _, _, err := remoteOwnedIdentityParts(p); err != nil {
		return false, err
	}
	members, err := remoteLinuxGroup(p.PID)
	if err != nil {
		return false, err
	}
	if len(members) == 0 {
		if _, err = remoteVerifyGroupLeader(p); err != nil {
			return false, err
		}
		return false, nil
	}
	verified, err := remoteVerifyGroupLeader(p)
	if err != nil {
		return true, err
	}
	if !verified {
		return true, errors.New("Owned process group remains but its original leader identity cannot be verified")
	}
	return true, nil
}
func remoteVerifyGroupMembers(members []remoteLinuxProcess, uid string, birth uint64) error {
	for _, member := range members {
		start, err := strconv.ParseUint(member.birth, 10, 64)
		if err != nil || member.uid != uid || start < birth {
			return errors.New("Process group contains unverifiable or unrelated members")
		}
	}
	return nil
}
func remoteSignalVerifiedMember(member remoteLinuxProcess, signal unix.Signal) error {
	fd, err := unix.PidfdOpen(member.pid, 0)
	if err == unix.ESRCH {
		return nil
	}
	if err != nil {
		return errors.New("Kernel cannot securely bind owned process cleanup")
	}
	defer unix.Close(fd)
	current, err := readRemoteLinuxProcess(member.pid)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.uid != member.uid || current.birth != member.birth || current.group != member.group {
		return errors.New("Owned group membership changed before cleanup")
	}
	if err = unix.PidfdSendSignal(fd, signal, nil, 0); err != nil && err != unix.ESRCH {
		return err
	}
	return nil
}
func stopRemoteOwnedProcess(p remoteOwnedProcess) error {
	uid, birth, err := remoteOwnedIdentityParts(p)
	if err != nil {
		return err
	}
	members, err := remoteLinuxGroup(p.PID)
	if err != nil {
		return err
	}
	verified, err := remoteVerifyGroupLeader(p)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	if !verified {
		return errors.New("Owned process group remains without a verifiable original leader; cleanup requires review")
	}
	if err = remoteVerifyGroupMembers(members, uid, birth); err != nil {
		return err
	}
	anchors := map[int]string{}
	for _, member := range members {
		anchors[member.pid] = member.birth
		if err = remoteSignalVerifiedMember(member, unix.SIGTERM); err != nil {
			return err
		}
	}
	termDeadline := time.Now().Add(2 * time.Second)
	killDeadline := termDeadline.Add(2 * time.Second)
	killed := false
	for {
		members, err = remoteLinuxGroup(p.PID)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}
		if err = remoteVerifyGroupMembers(members, uid, birth); err != nil {
			return err
		}
		anchored := false
		for _, member := range members {
			if anchors[member.pid] == member.birth {
				anchored = true
			}
		}
		if !anchored {
			return errors.New("Original owned group members disappeared before descendant ownership could be verified")
		}
		for _, member := range members {
			anchors[member.pid] = member.birth
		}
		if !time.Now().Before(termDeadline) {
			for _, member := range members {
				if err = remoteSignalVerifiedMember(member, unix.SIGKILL); err != nil {
					return err
				}
			}
			killed = true
		}
		if killed && !time.Now().Before(killDeadline) {
			return errors.New("Owned process group did not terminate within cleanup deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

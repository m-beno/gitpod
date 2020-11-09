// Copyright (c) 2020 TypeFox GmbH. All rights reserved.
// Licensed under the GNU Affero General Public License (AGPL).
// See License-AGPL.txt in the project root for license information.

package content

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/pkg/archive"
	"github.com/gitpod-io/gitpod/common-go/log"
	"github.com/gitpod-io/gitpod/common-go/tracing"
	csapi "github.com/gitpod-io/gitpod/content-service/api"
	wsinit "github.com/gitpod-io/gitpod/content-service/pkg/initializer"
	"github.com/gitpod-io/gitpod/content-service/pkg/storage"

	"github.com/golang/protobuf/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/opentracing/opentracing-go"

	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
)

// RunInitializerOpts configure RunInitializer
type RunInitializerOpts struct {
	// Command is the path to the initializer executable we'll run
	Command string
	// Args is a set of additional arguments to pass to the initializer executable
	Args []string

	UID uint32
	GID uint32
}

func collectRemoteContent(ctx context.Context, rs storage.DirectAccess, ps storage.PresignedAccess, workspaceOwner string, initializer *csapi.WorkspaceInitializer) (rc map[string]storage.DownloadInfo, err error) {
	rc = make(map[string]storage.DownloadInfo)

	backup, err := ps.SignDownload(ctx, rs.Bucket(workspaceOwner), rs.BackupObject(storage.DefaultBackup))
	if err == storage.ErrNotFound {
		// no backup found - that's fine
	} else if err != nil {
		return nil, err
	} else {
		rc[storage.DefaultBackup] = *backup
	}

	if si := initializer.GetSnapshot(); si != nil {
		bkt, obj, err := storage.ParseSnapshotName(si.Snapshot)
		if err != nil {
			return nil, err
		}
		info, err := ps.SignDownload(ctx, bkt, obj)
		if err != nil {
			return nil, xerrors.Errorf("cannot find snapshot: %w", err)
		}

		rc[storage.DefaultBackup] = *info
	}
	if si := initializer.GetPrebuild(); si != nil && si.Prebuild != nil {
		bkt, obj, err := storage.ParseSnapshotName(si.Prebuild.Snapshot)
		if err != nil {
			return nil, err
		}
		info, err := ps.SignDownload(ctx, bkt, obj)
		if err != nil {
			return nil, xerrors.Errorf("cannot find prebuild: %w", err)
		}

		rc[storage.DefaultBackup] = *info
	}

	return rc, nil
}

// RunInitializerC runs a content initializer in a user, PID and mount namespace to isolate it from ws-daemon
func RunInitializerC(ctx context.Context, destination string, initializer *csapi.WorkspaceInitializer, remoteContent map[string]storage.DownloadInfo, opts RunInitializerOpts) (err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "RunInitializer")
	defer tracing.FinishSpan(span, &err)

	init, err := proto.Marshal(initializer)
	if err != nil {
		return err
	}

	var inNSUID uint32 = 1
	uidMapping := []specs.LinuxIDMapping{
		{HostID: uint32(os.Getuid()), ContainerID: 0, Size: 1},
		{HostID: wsinit.GitpodUID, ContainerID: 1, Size: 1},
	}
	if opts.UID != wsinit.GitpodUID {
		uidMapping = append(uidMapping, specs.LinuxIDMapping{
			ContainerID: 2,
			HostID:      opts.UID,
			Size:        1,
		})
		inNSUID = 2
	}
	var inNSGID uint32 = 1
	gidMapping := []specs.LinuxIDMapping{
		{HostID: uint32(os.Getuid()), ContainerID: 0, Size: 1},
		{HostID: wsinit.GitpodUID, ContainerID: 1, Size: 1},
	}
	if opts.GID != wsinit.GitpodGID {
		uidMapping = append(gidMapping, specs.LinuxIDMapping{
			ContainerID: 2,
			HostID:      opts.GID,
			Size:        1,
		})
		inNSGID = 2
	}

	tmpdir, err := ioutil.TempDir("", "content-init")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	err = os.MkdirAll(filepath.Join(tmpdir, "rootfs"), 0755)
	if err != nil {
		return err
	}

	msg := msgInitContent{
		Destination:   "/dst",
		Initializer:   init,
		RemoteContent: remoteContent,
		TraceInfo:     tracing.GetTraceID(span),
	}
	fc, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filepath.Join(tmpdir, "rootfs", "content.json"), fc, 0644)
	if err != nil {
		return err
	}

	cmd := exec.Command("runc", "spec", "--rootless")
	cmd.Dir = tmpdir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return xerrors.Errorf("cannot create runc spec: %s: %w", string(out), err)
	}

	cfgFN := filepath.Join(tmpdir, "config.json")
	var spec specs.Spec
	fc, err = ioutil.ReadFile(cfgFN)
	if err != nil {
		return err
	}
	err = json.Unmarshal(fc, &spec)
	if err != nil {
		return err
	}

	// we assemble the root filesystem from the ws-daemon container
	for _, d := range []string{"app", "bin", "dev", "etc", "lib", "media", "opt", "sbin", "sys", "usr", "var"} {
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: "/" + d,
			Source:      "/" + d,
			Type:        "bind",
			Options:     []string{"rbind", "rprivate"},
		})
	}
	spec.Hostname = "content-init"
	spec.Process.NoNewPrivileges = true
	spec.Linux.UIDMappings = uidMapping
	spec.Linux.GIDMappings = gidMapping
	spec.Process.User.UID = inNSUID
	spec.Process.User.GID = inNSGID
	spec.Process.Args = []string{"/app/content-initializer"}

	fc, err = json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(cfgFN, fc, 0644)
	if err != nil {
		return err
	}

	cmd = exec.Command("runc", "run", "--root", filepath.Join(tmpdir, "state"), "content-init")
	cmd.Dir = tmpdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

// RunInitializer runs a content initializer in a user, PID and mount namespace to isolate it from ws-daemon
func RunInitializer(ctx context.Context, destination string, initializer *csapi.WorkspaceInitializer, remoteContent map[string]storage.DownloadInfo, opts RunInitializerOpts) (err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "RunInitializer")
	defer tracing.FinishSpan(span, &err)

	init, err := proto.Marshal(initializer)
	if err != nil {
		return err
	}

	if opts.Command == "" {
		opts.Command = "/proc/self/exe"
		opts.Args = []string{"content-initializer"}
	}
	if opts.UID == 0 {
		opts.UID = wsinit.GitpodUID
	}
	if opts.GID == 0 {
		opts.GID = wsinit.GitpodGID
	}

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer pipeW.Close()

	wd, err := ioutil.TempDir("", "content-init")
	if err != nil {
		return err
	}
	// It's ok to remove this directory because the child process acts in its own mount namespace.
	// All operations that need to persist will happen in a mount within that namespace, and not
	// directly in wd.
	defer os.RemoveAll(wd)

	inNSUID := 1
	uidMapping := []syscall.SysProcIDMap{
		{HostID: os.Getuid(), ContainerID: 0, Size: 1},
		{HostID: wsinit.GitpodUID, ContainerID: 1, Size: 1},
	}
	if opts.UID != wsinit.GitpodUID {
		uidMapping = append(uidMapping, syscall.SysProcIDMap{
			ContainerID: 2,
			HostID:      int(opts.UID),
			Size:        1,
		})
		inNSUID = 2
	}
	inNSGID := 1
	gidMapping := []syscall.SysProcIDMap{
		{HostID: os.Getuid(), ContainerID: 0, Size: 1},
		{HostID: wsinit.GitpodUID, ContainerID: 1, Size: 1},
	}
	if opts.GID != wsinit.GitpodGID {
		uidMapping = append(gidMapping, syscall.SysProcIDMap{
			ContainerID: 2,
			HostID:      int(opts.GID),
			Size:        1,
		})
		inNSGID = 2
	}

	cmd := exec.Command(opts.Command, opts.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig:                  syscall.SIGKILL,
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS,
		UidMappings:                uidMapping,
		GidMappings:                gidMapping,
		GidMappingsEnableSetgroups: true,
	}
	cmd.ExtraFiles = []*os.File{pipeR}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
	}
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "JAEGER_") {
			continue
		}
		cmd.Env = append(cmd.Env, e)
	}
	err = cmd.Start()
	if err != nil {
		return err
	}
	defer func() {
		if cmd.Process == nil {
			return
		}
		cmd.Process.Kill()
	}()

	err = writeMessage(pipeW, msgInitContent{
		Destination:   destination,
		RemoteContent: remoteContent,
		Initializer:   init,
		TraceInfo:     tracing.GetTraceID(span),
		UID:           inNSUID,
		GID:           inNSGID,
		Workdir:       wd,
	})
	if err != nil {
		return err
	}
	log.WithField("pid", cmd.Process.Pid).Debug("RunInitializer: started content init")

	err = cmd.Wait()
	if err != nil {
		return err
	}

	return nil
}

// RunInitializerChild is the function that's exepcted to run when we call `/proc/self/exe content-initializer`
func RunInitializerChild() (err error) {
	// FD 3 is the parent's extra file
	pipeR := os.NewFile(uintptr(3), "")

	var initmsg msgInitContent
	err = receiveMessageTimeout(pipeR, &initmsg, 30*time.Second)
	if err != nil {
		return err
	}

	span := opentracing.StartSpan("RunInitializerChild", opentracing.FollowsFrom(tracing.FromTraceID(initmsg.TraceInfo)))
	defer tracing.FinishSpan(span, &err)
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	var req csapi.WorkspaceInitializer
	err = proto.Unmarshal(initmsg.Initializer, &req)
	if err != nil {
		return err
	}

	// Setup our new home in the working dir.
	// The parent ensures that we have free reign here.
	for _, d := range []string{"bin", "dev", "etc", "lib", "media", "opt", "sbin", "sys", "usr", "var"} {
		dd := filepath.Join(initmsg.Workdir, d)
		err = os.MkdirAll(dd, 0755)
		if err != nil {
			return xerrors.Errorf("cannot mkdir %s: %w", dd, err)
		}

		err = unix.Mount("/"+d, dd, "none", unix.MS_BIND|unix.MS_REC, "")
		if err != nil {
			return xerrors.Errorf("cannot bind mount %s: %w", d, err)
		}
	}

	dst := filepath.Join(initmsg.Workdir, "dst")
	err = os.MkdirAll(dst, 0755)
	if err != nil {
		return xerrors.Errorf("cannot mkdir dst: %w", err)
	}
	// it's possible the destination folder doesn't exist yet, because the kubelet hasn't created it yet.
	// If we fail to create the folder, it either already exists, or we'll fail when we try and mount it.
	err = os.MkdirAll(initmsg.Destination, 0755)
	if err != nil && !os.IsExist(err) {
		return xerrors.Errorf("cannot mkdir destination: %w", err)
	}

	err = unix.Mount(initmsg.Destination, dst, "none", unix.MS_BIND|unix.MS_REC, "")
	if err != nil {
		return xerrors.Errorf("cannot bind mount %s to dst: %w", initmsg.Destination, err)
	}
	err = pivotRoot(initmsg.Workdir)
	if err != nil {
		return xerrors.Errorf("cannot pivot root to %s: %w", initmsg.Workdir, err)
	}

	// no_new_privs prevents anyone from regaining capabilities once we've executed setuid/setgid.
	err = unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
	if err != nil {
		return xerrors.Errorf("cannot set no_new_privs: %w", err)
	}

	// We setuid to the target user. This will also drop all capabilities, thereby preventing
	// anyone from regaining them or being able to setuid(0).
	err = unix.Setuid(initmsg.UID)
	if err != nil {
		return xerrors.Errorf("cannot setuid: %w", err)
	}
	err = unix.Setgid(initmsg.GID)
	if err != nil {
		return xerrors.Errorf("cannot setgid: %w", err)
	}

	rs := &remoteContentStorage{RemoteContent: initmsg.RemoteContent}

	initializer, err := wsinit.NewFromRequest(ctx, "/dst", rs, &req)
	if err != nil {
		return err
	}

	initSource, err := wsinit.InitializeWorkspace(ctx, "/dst", rs,
		wsinit.WithInitializer(initializer),
		wsinit.WithCleanSlate,
		wsinit.WithChown(initmsg.UID, initmsg.GID),
	)
	if err != nil {
		return err
	}

	// Place the ready file to make Theia "open its gates"
	err = wsinit.PlaceWorkspaceReadyFile(ctx, "/dst", initSource, 0, 0)
	if err != nil {
		return err
	}

	return nil
}

// chroot will call chrot such that rootfs becomes the new root
// filesystem, and everything else is cleaned up.
//
// Note: unlike runc's pivot_root we use chroot here, because chroot does not require
//       the relationship between new_root and put_old. After unmounting the old root,
//       we won't have access to /proc anymore, hence cannot change back into the old
//       root anyways.
//
// copied from runc: https://github.com/opencontainers/runc/blob/cf6c074115d00c932ef01dedb3e13ba8b8f964c3/libcontainer/rootfs_linux.go#L760
func chroot(rootfs string) error {
	oldroot, err := unix.Open("/", unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(oldroot)

	newroot, err := unix.Open(rootfs, unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(newroot)

	// Change to the new root so that the pivot_root actually acts on it.
	if err := unix.Fchdir(newroot); err != nil {
		return err
	}

	if err := unix.Chroot(rootfs); err != nil {
		return fmt.Errorf("pivot_root %s", err)
	}

	// Currently our "." is oldroot (according to the current kernel code).
	// However, purely for safety, we will fchdir(oldroot) since there isn't
	// really any guarantee from the kernel what /proc/self/cwd will be after a
	// pivot_root(2).

	if err := unix.Fchdir(oldroot); err != nil {
		return err
	}

	// Make oldroot rslave to make sure our unmounts don't propagate to the
	// host (and thus bork the machine). We don't use rprivate because this is
	// known to cause issues due to races where we still have a reference to a
	// mount while a process in the host namespace are trying to operate on
	// something they think has no mounts (devicemapper in particular).
	if err := unix.Mount("", ".", "", unix.MS_SLAVE|unix.MS_REC, ""); err != nil {
		return err
	}
	// Preform the unmount. MNT_DETACH allows us to unmount /proc/self/cwd.
	if err := unix.Unmount(".", unix.MNT_DETACH); err != nil {
		return err
	}

	// Switch back to our shiny new root.
	if err := unix.Chdir("/"); err != nil {
		return fmt.Errorf("chdir / %s", err)
	}
	return nil
}

// pivotRoot will call pivot_root such that rootfs becomes the new root
// filesystem, and everything else is cleaned up.
//
// copied from runc: https://github.com/opencontainers/runc/blob/cf6c074115d00c932ef01dedb3e13ba8b8f964c3/libcontainer/rootfs_linux.go#L760
func pivotRoot(rootfs string) error {
	// While the documentation may claim otherwise, pivot_root(".", ".") is
	// actually valid. What this results in is / being the new root but
	// /proc/self/cwd being the old root. Since we can play around with the cwd
	// with pivot_root this allows us to pivot without creating directories in
	// the rootfs. Shout-outs to the LXC developers for giving us this idea.

	oldroot, err := unix.Open("/", unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(oldroot)

	newroot, err := unix.Open(rootfs, unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(newroot)

	// Change to the new root so that the pivot_root actually acts on it.
	if err := unix.Fchdir(newroot); err != nil {
		return err
	}

	if err := unix.PivotRoot(".", "."); err != nil {
		return fmt.Errorf("pivot_root %s", err)
	}

	// Currently our "." is oldroot (according to the current kernel code).
	// However, purely for safety, we will fchdir(oldroot) since there isn't
	// really any guarantee from the kernel what /proc/self/cwd will be after a
	// pivot_root(2).

	if err := unix.Fchdir(oldroot); err != nil {
		return err
	}

	// Make oldroot rslave to make sure our unmounts don't propagate to the
	// host (and thus bork the machine). We don't use rprivate because this is
	// known to cause issues due to races where we still have a reference to a
	// mount while a process in the host namespace are trying to operate on
	// something they think has no mounts (devicemapper in particular).
	if err := unix.Mount("", ".", "", unix.MS_SLAVE|unix.MS_REC, ""); err != nil {
		return err
	}
	// Preform the unmount. MNT_DETACH allows us to unmount /proc/self/cwd.
	if err := unix.Unmount(".", unix.MNT_DETACH); err != nil {
		return err
	}

	// Switch back to our shiny new root.
	if err := unix.Chdir("/"); err != nil {
		return fmt.Errorf("chdir / %s", err)
	}
	return nil
}

type remoteContentStorage struct {
	RemoteContent map[string]storage.DownloadInfo
}

// Init does nothing
func (rs *remoteContentStorage) Init(ctx context.Context, owner, workspace string) error {
	return nil
}

// EnsureExists does nothing
func (rs *remoteContentStorage) EnsureExists(ctx context.Context) error {
	return nil
}

// Download always returns false and does nothing
func (rs *remoteContentStorage) Download(ctx context.Context, destination string, name string) (exists bool, err error) {
	info, exists := rs.RemoteContent[name]
	if !exists {
		return false, nil
	}

	resp, err := http.Get(info.URL)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()

	err = archive.Untar(resp.Body, destination, &archive.TarOptions{})
	if err != nil {
		return true, err
	}

	return true, nil
}

// DownloadSnapshot always returns false and does nothing
func (rs *remoteContentStorage) DownloadSnapshot(ctx context.Context, destination string, name string) (bool, error) {
	return rs.Download(ctx, destination, name)
}

// Qualify just returns the name
func (rs *remoteContentStorage) Qualify(name string) string {
	return name
}

// Upload does nothing
func (rs *remoteContentStorage) Upload(ctx context.Context, source string, name string, opts ...storage.UploadOption) (string, string, error) {
	return "", "", fmt.Errorf("not implemented")
}

// Bucket returns an empty string
func (rs *remoteContentStorage) Bucket(string) string {
	return ""
}

// BackupObject returns a backup's object name that a direct downloader would download
func (rs *remoteContentStorage) BackupObject(name string) string {
	return ""
}

// SnapshotObject returns a snapshot's object name that a direct downloer would download
func (rs *remoteContentStorage) SnapshotObject(name string) string {
	return ""
}

type msgInitContent struct {
	Destination   string
	RemoteContent map[string]storage.DownloadInfo
	Initializer   []byte
	TraceInfo     string
	UID, GID      int
	Workdir       string
}

const (
	maxLength = 1 << 16
)

// writeMessage serializes a message to the writer.
// copied from https://github.com/rootless-containers/rootlesskit/blob/7fd0654f328e20b4b6c1649620fd9776d852da13/pkg/msgutil/msgutil.go
func writeMessage(out io.Writer, msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(b) > maxLength {
		return xerrors.Errorf("bad message length: %d (max: %d)", len(b), maxLength)
	}
	h := make([]byte, 4)
	binary.LittleEndian.PutUint32(h, uint32(len(b)))
	_, err = out.Write(append(h, b...))
	if err != nil {
		return err
	}

	return nil
}

// receiveMessage deserializes a message from the reader.
// copied from https://github.com/rootless-containers/rootlesskit/blob/7fd0654f328e20b4b6c1649620fd9776d852da13/pkg/msgutil/msgutil.go
func receiveMessage(in io.Reader, msg interface{}) error {
	hdr := make([]byte, 4)
	n, err := in.Read(hdr)
	if err != nil {
		return err
	}
	if n != 4 {
		return xerrors.Errorf("read %d bytes, expected 4 bytes", n)
	}
	bLen := binary.LittleEndian.Uint32(hdr)
	if bLen > maxLength || bLen < 1 {
		return xerrors.Errorf("bad message length: %d (max: %d)", bLen, maxLength)
	}
	b := make([]byte, bLen)
	n, err = in.Read(b)
	if err != nil {
		return err
	}
	if n != int(bLen) {
		return xerrors.Errorf("read %d bytes, expected %d bytes", n, bLen)
	}
	return json.Unmarshal(b, msg)
}

func receiveMessageTimeout(in io.Reader, msg interface{}, timeout time.Duration) error {
	errchan := make(chan error, 1)
	incoming := make(chan struct{})
	go func() {
		log.Debug("waiting for parent message")

		err := receiveMessage(in, msg)
		if err != nil {
			errchan <- err
			return
		}
		close(incoming)
	}()

	select {
	case err := <-errchan:
		return err
	case <-time.After(timeout):
		return xerrors.Errorf("did not receive message in time")
	case <-incoming:
	}
	return nil
}

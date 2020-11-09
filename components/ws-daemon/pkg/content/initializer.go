// Copyright (c) 2020 TypeFox GmbH. All rights reserved.
// Licensed under the GNU Affero General Public License (AGPL).
// See License-AGPL.txt in the project root for license information.

package content

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/docker/docker/pkg/archive"
	"github.com/gitpod-io/gitpod/common-go/tracing"
	csapi "github.com/gitpod-io/gitpod/content-service/api"
	wsinit "github.com/gitpod-io/gitpod/content-service/pkg/initializer"
	"github.com/gitpod-io/gitpod/content-service/pkg/storage"

	"github.com/golang/protobuf/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/opentracing/opentracing-go"

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

// RunInitializer runs a content initializer in a user, PID and mount namespace to isolate it from ws-daemon
func RunInitializer(ctx context.Context, destination string, initializer *csapi.WorkspaceInitializer, remoteContent map[string]storage.DownloadInfo, opts RunInitializerOpts) (err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "RunInitializer")
	defer tracing.FinishSpan(span, &err)

	// it's possible the destination folder doesn't exist yet, because the kubelet hasn't created it yet.
	// If we fail to create the folder, it either already exists, or we'll fail when we try and mount it.
	err = os.MkdirAll(destination, 0755)
	if err != nil && !os.IsExist(err) {
		return xerrors.Errorf("cannot mkdir destination: %w", err)
	}

	init, err := proto.Marshal(initializer)
	if err != nil {
		return err
	}

	if opts.GID == 0 {
		opts.GID = wsinit.GitpodGID
	}
	if opts.UID == 0 {
		opts.UID = wsinit.GitpodUID
	}

	tmpdir, err := ioutil.TempDir("", "content-init")
	if err != nil {
		return err
	}
	// defer os.RemoveAll(tmpdir)

	err = os.MkdirAll(filepath.Join(tmpdir, "rootfs"), 0755)
	if err != nil {
		return err
	}

	msg := msgInitContent{
		Destination:   "/dst",
		Initializer:   init,
		RemoteContent: remoteContent,
		TraceInfo:     tracing.GetTraceID(span),
		GID:           int(opts.GID),
		UID:           int(opts.UID),
	}
	fc, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filepath.Join(tmpdir, "rootfs", "content.json"), fc, 0644)
	if err != nil {
		return err
	}

	cmd := exec.Command("runc", "spec")
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
	for _, d := range []string{"app", "bin", "dev", "etc", "lib", "opt", "sbin", "sys", "usr", "var"} {
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: "/" + d,
			Source:      "/" + d,
			Type:        "bind",
			Options:     []string{"rbind", "rprivate"},
		})
	}
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: "/dst",
		Source:      destination,
		Type:        "bind",
		Options:     []string{"bind", "rprivate"},
	})

	spec.Hostname = "content-init"
	spec.Process.Terminal = false
	spec.Process.NoNewPrivileges = true
	spec.Process.User.UID = wsinit.GitpodUID
	spec.Process.User.GID = wsinit.GitpodGID
	spec.Process.Args = []string{"/app/content-initializer"}

	fc, err = json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(cfgFN, fc, 0644)
	if err != nil {
		return err
	}

	cmd = exec.Command("runc", "--root", "state", "--debug", "--log-format", "json", "run", "gogogo")
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

// RunInitializerChild is the function that's exepcted to run when we call `/proc/self/exe content-initializer`
func RunInitializerChild() (err error) {
	fc, err := ioutil.ReadFile("/content.json")
	if err != nil {
		return err
	}

	var initmsg msgInitContent
	json.Unmarshal(fc, &initmsg)
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

	rs := &remoteContentStorage{RemoteContent: initmsg.RemoteContent}

	initializer, err := wsinit.NewFromRequest(ctx, "/dst", rs, &req)
	if err != nil {
		return err
	}

	initSource, err := wsinit.InitializeWorkspace(ctx, "/dst", rs,
		wsinit.WithInitializer(initializer),
		wsinit.WithCleanSlate,
		wsinit.WithChown(initmsg.UID, initmsg.GID),
		wsinit.WithIgnoreChownErrors,
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

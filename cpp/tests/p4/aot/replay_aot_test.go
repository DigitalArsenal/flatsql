// The replay's WASM-AOT arm (flatsql-p4-replay-10x-20261018): the p4 test
// command (flatsql-p4-test.wasm, built as the shipped flatsql-p4-threads.wasm
// is: Linux wasi-sdk 30, the same engine library and flags) run as SDN runs the
// format-4 engine. sdn-server's flatsqlrt compiles it with the node's
// threaded, interruptible AOT configuration and runs it on wasmrt with the
// node's native host I/O (one confined store root, the writer class, the
// node's fd budget) and its wasi-threads.
// The test client inside the command drives the engine through the mailbox, as
// SDN's storage/format4 does.
//
// This file is not part of flatsql's build: cpp/tests/p4-replay.e2e.mjs
// overlays it into sdn-server/internal/flatsqlrt (go test -overlay), so the SDN
// checkout stays untouched, and runs the compiled test once per measurement:
//
//	P4_REPLAY_AOT_WASM   the test command
//	P4_REPLAY_AOT_CACHE  the AOT cache directory (keyed on the wasm bytes)
//	P4_REPLAY_AOT_ROOT   the store root: every path the command uses is under it
//	P4_REPLAY_AOT_ARGS   the command's arguments, separated by \x1f
package flatsqlrt

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spacedatanetwork/sdn-server/internal/wasmrt"
)

func TestP4ReplayAOT(t *testing.T) {
	wasmPath := os.Getenv("P4_REPLAY_AOT_WASM")
	if wasmPath == "" {
		t.Skip("P4_REPLAY_AOT_WASM not set")
	}
	if !NativeHostIOSupported() {
		t.Fatal("native host I/O unavailable")
	}
	root, err := filepath.EvalSymlinks(os.Getenv("P4_REPLAY_AOT_ROOT"))
	if err != nil {
		t.Fatal(err)
	}
	wasm, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatal(err)
	}
	// The command's own maximum (4 GiB): the shipped engine's is 2 GiB, the
	// test client's records and answers live beside it.
	const maxPages = 65536
	aot, _, err := EnsureThreadedAOTArtifact(os.Getenv("P4_REPLAY_AOT_CACHE"), "fsqlp4replay", wasm, true)
	if err != nil {
		t.Fatal(err)
	}
	processNoFile()
	store, err := OpenNativeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Release()
	budget, _ := P4FDBudget(0)
	io, err := NewNativeHostIO(store, HostIOWriter, budget, psDefaultMaxHandles)
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"flatsql-p4-test.wasm"}, strings.Split(os.Getenv("P4_REPLAY_AOT_ARGS"), "\x1f")...)
	mod, err := wasmrt.NewModule(aot,
		wasmrt.WithWASIArgs(args, nil, []string{root + ":" + root}),
		wasmrt.WithMaxMemoryPages(maxPages),
		wasmrt.WithMaxThreads(64),
		wasmrt.WithServiceThreads(),
		wasmrt.WithEnvInstaller(io.Install),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := mod.Execute("_start")
	mod.Release()
	io.Revoke()
	io.ReleaseParked()
	io.Reap()
	_ = io.Free()
	if runErr != nil {
		t.Fatalf("_start: %v", runErr)
	}
}

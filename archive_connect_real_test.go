package aergo

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestConnectArchiveRealArchive connects to a Java archive of each version
// listed in AERGO_ARCHIVE_CLASSPATHS: classpaths (aeron-archive, aeron-driver,
// aeron-client and agrona jars), separated by ";". The fake archive in the
// other tests answers whatever message the test expects, so it cannot say
// whether a real archive accepts the connect request. Archive 1.53 dropped
// the plain ConnectRequest, and only a real one showed it.
//
//	AERGO_ARCHIVE_CLASSPATHS="$cp152;$cp153" go test -run TestConnectArchiveRealArchive
//
// It needs java on the PATH, and skips without the variable.
func TestConnectArchiveRealArchive(t *testing.T) {
	list := os.Getenv("AERGO_ARCHIVE_CLASSPATHS")
	if list == "" {
		t.Skip("set AERGO_ARCHIVE_CLASSPATHS to run against a real archive")
	}
	for _, cp := range strings.Split(list, ";") {
		name := filepath.Base(strings.Split(cp, ":")[0])
		t.Run(name, func(t *testing.T) { connectToRealArchive(t, cp) })
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

func connectToRealArchive(t *testing.T, classpath string) {
	dir := t.TempDir()
	aeronDir := filepath.Join(dir, "aeron")
	controlPort, responsePort := freeUDPPort(t), freeUDPPort(t)

	cmd := exec.Command("java",
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
		"-cp", classpath,
		"-Daeron.dir="+aeronDir,
		"-Daeron.archive.dir="+filepath.Join(dir, "archive"),
		fmt.Sprintf("-Daeron.archive.control.channel=aeron:udp?endpoint=127.0.0.1:%d", controlPort),
		"-Daeron.archive.replication.channel=aeron:udp?endpoint=127.0.0.1:0",
		"-Daeron.threading.mode=SHARED",
		"io.aeron.archive.ArchivingMediaDriver")
	out, err := os.Create(filepath.Join(dir, "driver.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the archive: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() {
			log, _ := os.ReadFile(filepath.Join(dir, "driver.log"))
			t.Logf("archive output:\n%s", log)
		}
	})

	var ac *Aeron
	deadline := time.Now().Add(30 * time.Second)
	for {
		ac, err = Connect(WithDir(aeronDir))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect to the driver: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Cleanup(func() { ac.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, err := ConnectArchive(ctx, ac, ArchiveConfig{
		ControlRequestChannel:  fmt.Sprintf("aeron:udp?endpoint=127.0.0.1:%d", controlPort),
		ControlResponseChannel: fmt.Sprintf("aeron:udp?endpoint=127.0.0.1:%d", responsePort),
	})
	if err != nil {
		t.Fatalf("connect with no credentials: %v", err)
	}
	defer a.Close()

	// A request after the connect proves the session works, not only the handshake.
	if _, err := a.ListRecordingsForUri(ctx, 0, 10, "", 183); err != nil {
		t.Fatalf("list recordings: %v", err)
	}
}

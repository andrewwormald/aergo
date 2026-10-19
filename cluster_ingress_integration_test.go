package aergo

import (
	"os"
	"testing"
)

// The cluster client's ingress publications are exclusive (single writer), as in
// Java's AeronCluster. Needs a media driver (AERGO_TEST_AERON_DIR); no cluster is
// required, since creating the publications does not need a peer.
func TestClusterIngressPublicationsAreExclusive(t *testing.T) {
	dir := os.Getenv("AERGO_TEST_AERON_DIR")
	if dir == "" {
		t.Skip("AERGO_TEST_AERON_DIR not set")
	}
	cfg := DefaultClusterConfig()
	cfg.AeronDir = dir
	cfg.Members = []ClusterMember{{MemberId: 0, Endpoint: "localhost:20110"}, {MemberId: 1, Endpoint: "localhost:20111"}}
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.createIngressPublications()

	if len(c.ingressPubs) != 2 {
		t.Fatalf("%d ingress publications, want 2", len(c.ingressPubs))
	}
	for i, p := range c.ingressPubs {
		if !p.exclusive {
			t.Errorf("ingress publication %d is shared", i)
		}
	}
}

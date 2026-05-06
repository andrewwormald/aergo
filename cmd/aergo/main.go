package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	aeron "github.com/andrewwormald/aergo/pkg/aeron/native"
	aeroncluster "github.com/andrewwormald/aergo/pkg/cluster"
	"github.com/andrewwormald/aergo/pkg/codec/cluster"
)

var (
	mode     = flag.String("mode", "cluster", "mode: cluster")
	aeronDir = flag.String("dir", "", "aeron media driver directory")
	endpoint = flag.String("endpoint", "localhost:10000", "cluster member endpoint")
)

func main() {
	flag.Parse()

	switch *mode {
	case "cluster":
		runCluster(*aeronDir, *endpoint)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runCluster(aeronDir, endpoint string) {
	cfg := aeroncluster.DefaultConfig()
	cfg.AeronDir = aeronDir
	cfg.Members = []aeroncluster.ClusterMember{
		{MemberId: 0, Endpoint: endpoint},
	}
	cfg.Listener = &clusterListener{}
	cfg.AutoReconnect = true
	cfg.LockOSThread = true

	cc, err := aeroncluster.New(cfg)
	if err != nil {
		log.Fatalf("failed to create cluster client: %v", err)
	}

	cc.Connect()
	log.Printf("connecting to cluster at %s...", endpoint)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigCh:
			log.Printf("graceful shutdown...")
			cc.GracefulClose()
			for i := 0; i < 10 && cc.State() != aeroncluster.StateClosed; i++ {
				cc.Poll()
				time.Sleep(100 * time.Millisecond)
			}
			cc.Close()
			return
		default:
		}

		cc.Poll()

		if cc.State() == aeroncluster.StateConnected {
			msg := []byte("hello cluster from aergo")
			result := cc.Offer(msg)
			if result > 0 {
				log.Printf("sent to cluster: position=%d", result)
			}
			time.Sleep(time.Second)
		} else if cc.State() == aeroncluster.StateClosed {
			return
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

type clusterListener struct{}

func (l *clusterListener) OnMessage(c aeroncluster.Cluster, timestamp int64, buffer []byte, offset int, length int) {
	log.Printf("cluster message: timestamp=%d payload=%d bytes", timestamp, length)
}

func (l *clusterListener) OnSessionEvent(c aeroncluster.Cluster, event *cluster.SessionEvent) {
	log.Printf("session event: code=%d detail=%s session=%d leader=%d",
		event.Code, event.Detail, event.ClusterSessionId, event.LeaderMemberId)
}

func (l *clusterListener) OnNewLeader(c aeroncluster.Cluster, event *cluster.NewLeaderEvent) {
	log.Printf("new leader: member=%d term=%d endpoints=%s",
		event.LeaderMemberId, event.LeadershipTermId, event.IngressEndpoints)
}

func (l *clusterListener) OnChallenge(c aeroncluster.Cluster, challenge *cluster.Challenge) []byte {
	log.Printf("challenge received: correlationId=%d, %d bytes", challenge.CorrelationId, len(challenge.ChallengeData))
	return nil
}

// Suppress unused import
var _ = aeron.Connect

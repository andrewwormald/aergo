package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrewwormald/aergo"
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
	cfg := aergo.DefaultClusterConfig()
	cfg.AeronDir = aeronDir
	cfg.Members = []aergo.ClusterMember{
		{MemberId: 0, Endpoint: endpoint},
	}
	cfg.Listener = &clusterListener{}
	cfg.AutoReconnect = true
	cfg.LockOSThread = true

	cc, err := aergo.NewCluster(cfg)
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
			for i := 0; i < 10 && cc.State() != aergo.StateClosed; i++ {
				cc.Poll()
				time.Sleep(100 * time.Millisecond)
			}
			cc.Close()
			return
		default:
		}

		cc.Poll()

		if cc.State() == aergo.StateConnected {
			msg := []byte("hello cluster from aergo")
			result := cc.Offer(msg)
			if result > 0 {
				log.Printf("sent to cluster: position=%d", result)
			}
			time.Sleep(time.Second)
		} else if cc.State() == aergo.StateClosed {
			return
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

type clusterListener struct{}

func (l *clusterListener) OnMessage(c aergo.Cluster, timestamp int64, buffer []byte, offset int, length int) {
	log.Printf("cluster message: timestamp=%d payload=%d bytes", timestamp, length)
}

func (l *clusterListener) OnSessionEvent(c aergo.Cluster, event *aergo.SessionEvent) {
	log.Printf("session event: code=%d detail=%s session=%d leader=%d",
		event.Code, event.Detail, event.ClusterSessionId, event.LeaderMemberId)
}

func (l *clusterListener) OnNewLeader(c aergo.Cluster, event *aergo.NewLeaderEvent) {
	log.Printf("new leader: member=%d term=%d endpoints=%s",
		event.LeaderMemberId, event.LeadershipTermId, event.IngressEndpoints)
}

func (l *clusterListener) OnChallenge(c aergo.Cluster, challenge *aergo.Challenge) []byte {
	log.Printf("challenge received: correlationId=%d, %d bytes", challenge.CorrelationId, len(challenge.ChallengeData))
	return nil
}

func (l *clusterListener) OnError(c aergo.Cluster, err error) {
	log.Printf("cluster error: %v", err)
}

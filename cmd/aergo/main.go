package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrewwormald/aergo/pkg/aeron/client"
	"github.com/andrewwormald/aergo/pkg/aeron/driver"
	aeroncluster "github.com/andrewwormald/aergo/pkg/cluster"
	"github.com/andrewwormald/aergo/pkg/codec/cluster"
)

var (
	mode     = flag.String("mode", "pubsub", "mode: pubsub, tryclaim, or cluster")
	libPath  = flag.String("lib", "", "path to libaeron.so/dylib")
	aeronDir = flag.String("dir", "", "aeron media driver directory")
	channel  = flag.String("channel", "aeron:udp?endpoint=localhost:40123", "aeron channel URI")
	streamId = flag.Int("stream", 1001, "aeron stream ID")
	endpoint = flag.String("endpoint", "localhost:10000", "cluster member endpoint")
)

func main() {
	flag.Parse()

	if err := driver.Open(*libPath); err != nil {
		log.Fatalf("failed to load aeron library: %v", err)
	}
	defer driver.Close()

	log.Printf("aeron version: %s", driver.VersionFull())

	switch *mode {
	case "pubsub":
		runPubSub(*aeronDir, *channel, int32(*streamId))
	case "tryclaim":
		runTryClaim(*aeronDir, *channel, int32(*streamId))
	case "cluster":
		runCluster(*aeronDir, *endpoint)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runPubSub(aeronDir, channel string, streamId int32) {
	opts := []client.Option{}
	if aeronDir != "" {
		opts = append(opts, client.WithDir(aeronDir))
	}

	c, err := client.New(opts...)
	if err != nil {
		log.Fatalf("failed to create aeron client: %v", err)
	}
	defer c.Close()

	pub, err := c.AddPublication(channel, streamId)
	if err != nil {
		log.Fatalf("failed to add publication: %v", err)
	}
	defer pub.Close()
	log.Printf("publication created: stream=%d", pub.StreamId())

	sub, err := c.AddSubscription(channel, streamId)
	if err != nil {
		log.Fatalf("failed to add subscription: %v", err)
	}
	defer sub.Close()
	log.Printf("subscription created: stream=%d", sub.StreamId())

	log.Printf("waiting for connection...")
	for !pub.IsConnected() {
		time.Sleep(10 * time.Millisecond)
	}
	log.Printf("connected")

	msg := []byte("hello from aergo")
	result := pub.OfferWithBackpressure(msg, client.BackpressureYield, 1000)
	if result > 0 {
		log.Printf("sent message: position=%d", result)
	} else {
		log.Fatalf("failed to send: %d", result)
	}

	received := false
	assembler := client.NewFragmentAssembler(func(buffer []byte, header *client.Header) {
		log.Printf("received: %s (%d bytes)", string(buffer), len(buffer))
		received = true
	})
	for i := 0; i < 100 && !received; i++ {
		sub.Poll(assembler.OnFragment, 10)
		if !received {
			time.Sleep(10 * time.Millisecond)
		}
	}

	if !received {
		log.Printf("warning: no message received")
	}
	log.Printf("pub/sub test complete")
}

func runTryClaim(aeronDir, channel string, streamId int32) {
	opts := []client.Option{}
	if aeronDir != "" {
		opts = append(opts, client.WithDir(aeronDir))
	}

	c, err := client.New(opts...)
	if err != nil {
		log.Fatalf("failed to create aeron client: %v", err)
	}
	defer c.Close()

	pub, err := c.AddPublication(channel, streamId)
	if err != nil {
		log.Fatalf("failed to add publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, streamId)
	if err != nil {
		log.Fatalf("failed to add subscription: %v", err)
	}
	defer sub.Close()

	for !pub.IsConnected() {
		time.Sleep(10 * time.Millisecond)
	}

	// Zero-copy write via TryClaim
	msg := []byte("zero-copy tryclaim msg")
	var claim *client.BufferClaim
	var pos int64
	for {
		claim, pos = pub.TryClaim(len(msg))
		if pos > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Write directly into the log buffer
	buf := claim.Buffer()
	copy(buf, msg)
	if err := claim.Commit(); err != nil {
		log.Fatalf("commit failed: %v", err)
	}
	log.Printf("tryclaim sent: position=%d", pos)

	// Read it back
	received := false
	for i := 0; i < 100 && !received; i++ {
		sub.Poll(func(buffer []byte, header *client.Header) {
			log.Printf("tryclaim received: %s (%d bytes)", string(buffer), len(buffer))
			received = true
		}, 10)
		if !received {
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Printf("tryclaim test complete")
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
			// Poll a few more times for close ack
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
	return nil // no auth in smoke test
}

func init() {
	client.SetErrorHandler(func(errcode int32, message string) {
		fmt.Fprintf(os.Stderr, "aeron error [%d]: %s\n", errcode, message)
	})
}

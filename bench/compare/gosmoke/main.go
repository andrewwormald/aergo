package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/andrewwormald/aergo"
)

func main() {
	c, err := aergo.Connect(aergo.WithDir(os.Args[1]))
	if err != nil {
		panic(err)
	}
	defer c.Close()
	pub, err := c.AddPublication("aeron:ipc", 1001)
	if err != nil {
		panic(err)
	}
	sub, err := c.AddSubscription("aeron:ipc", 1001)
	if err != nil {
		panic(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !pub.IsConnected() && time.Now().Before(deadline) {
		c.DoWork()
		time.Sleep(time.Millisecond)
	}
	pos, err := pub.OfferWithBackoff(context.Background(), []byte("hello"), time.Second)
	fmt.Println("connected:", pub.IsConnected(), "offer:", pos, err)
	got := 0
	for i := 0; i < 1000 && got == 0; i++ {
		c.DoWork()
		got = sub.Poll(func(b []byte, h *aergo.Header) { fmt.Printf("received %q\n", b) }, 10)
		time.Sleep(time.Millisecond)
	}
}

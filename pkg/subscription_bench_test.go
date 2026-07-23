package aeron

import "testing"

func BenchmarkSubscriptionPoll(b *testing.B) {
	termLen := int32(64 * 1024)
	lb := newInMemLogBuffers(termLen)
	app := NewTermAppender(lb, 0)
	payload := make([]byte, 32)
	const frames = 128
	for i := 0; i < frames; i++ {
		app.Append(0, 1, 1001, payload)
	}

	sub := newInMemSubscription(lb, 1001)
	img := sub.conductor.subscriptions[sub.registrationID].images[0]
	noop := func(buf []byte, h *Header) {}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		img.subscriberPosition = 0
		sub.Poll(noop, frames)
	}
}

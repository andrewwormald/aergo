.PHONY: bench bench-quick

# Full baseline run: 10 counts × 2s/bench, written to bench/baseline.txt.
bench:
	@mkdir -p bench
	go test -bench=. -benchmem -benchtime=2s -count=10 -run=^$$ ./pkg/aeron/... ./pkg/cluster/... | tee bench/baseline.txt

# Smoke run for development; ~1 minute end-to-end.
bench-quick:
	go test -bench=. -benchmem -benchtime=1s -count=1 -run=^$$ ./pkg/aeron/... ./pkg/cluster/...

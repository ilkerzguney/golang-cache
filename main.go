package main

import (
	"cache/cache"
	"flag"
)

func main() {
	var (
     	listenAddr = flag.String("listenaddr", ":3000", "listen address of the server")
     	leaderAddr = flag.String("leaderaddr", "", "listen address of the leader")
	)
	flag.Parse()

	opts := ServerOpts{
		ListenAddr: *listenAddr,
		IsLeader: len(*leaderAddr) > 0,
		LeaderAddr: *leaderAddr,
	}

	server := NewServer(opts, cache.New(), opts.IsLeader ? :)
	server.Start()
}
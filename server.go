package main

import (
	"cache/cache"
	"context"
	"fmt"
	"log"
	"net"
)

type ServerOpts struct {
	ListenAddr string
	IsLeader bool
	LeaderAddr string
}

type Server struct {
	ServerOpts

	followers map[net.Conn]struct{}

	cache cache.Cacher
}

func NewServer(opts ServerOpts, c cache.Cacher) *Server {
	return &Server {
		ServerOpts: opts,
		cache: c,
		// TODO: only allcoate when we are leader
		followers: make(map[net.Conn]struct{}),
	}
}

func (s *Server) Start() error {
   ln, err := net.Listen("tcp", s.ListenAddr)
   if err != nil {
	return fmt.Errorf("listen error: %s", err)
   }
   
   log.Printf("server starting on port [%s]\n", s.ListenAddr)
   
   if !s.IsLeader {
		conn, err := net.Dial("tcp", s.LeaderAddr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("connected with leader:", s.LeaderAddr)
		s.handleConn(conn)
   }
   
   for {
	 conn, err := ln.Accept()
	 if err != nil {
		log.Printf("accept error: %s\n", err)
		continue
	 }
	 go s.handleConn(conn)
   }
}

func (s *Server) handleConn(conn net.Conn) {
   defer conn.Close()

   buf := make([]byte, 2048)

   if s.IsLeader {
	s.followers[conn] = struct{}{}
   }

   for {
		n, err := conn.Read(buf)
        if err != nil {
			log.Printf("conn read error: %s\n", err)
			break;
		}
        go s.handleCommand(conn, buf[:n])
   }
}

func (s *Server) handleCommand(conn net.Conn, rawCmd []byte) {
	var err error
	msg, err := parseMessage(rawCmd)
	if err != nil {
		fmt.Println("failed to parse command", err)
		conn.Write([]byte(err.Error()))
		return
	}

	switch msg.Cmd {
	case CMDSet:
	    err := s.handleSetCmd(conn, msg)
		if err != nil {
			fmt.Printf("error occurred during SET command: %s\n", err.Error())
		}
	case CMDGet:
		err := s.handleGetCmd(conn, msg)
		if err != nil {
			fmt.Printf("error occurred during GET command: %s\n", err.Error())
		}
	}
}

func (s *Server) handleSetCmd(conn net.Conn, msg *Message) error {
	fmt.Println("handling SET command: ", msg)
	if err := s.cache.Set(msg.Key, msg.Value, msg.TTL); err != nil {
		return err
	}

    go s.sendToFollowers(context.TODO(), msg)
	return nil
}

func (s *Server) handleGetCmd(conn net.Conn, msg *Message) error {
	fmt.Println("handling GET command: ", msg)
	val, err := s.cache.Get(msg.Key);
	if err != nil {
		return err
	}

    _, err = conn.Write(val)

	return err
}

func (s *Server) sendToFollowers(ctx context.Context, msg *Message) error {
	for conn := range s.followers {
		_, err := conn.Write(msg.ToBytes())
		if err != nil {
			log.Println("write to follower error:", err)
			continue
		}
	}
	return nil
}
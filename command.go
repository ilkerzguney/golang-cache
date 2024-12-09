package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

type Command string

const (
	CMDSet Command = "SET"
	CMDGet Command = "GET"
)

type MSGSet struct {
	Key []byte
	Value []byte
	TTL time.Duration
}

type MSGGet struct {
	Key []byte
}

type Message struct {
	Cmd   Command
	Key   []byte
	Value []byte
	TTL   time.Duration
}

func (m *Message) ToBytes() []byte {
	switch m.Cmd {
	case CMDSet:
		cmd := fmt.Sprintf("%s %s %s %d", m.Cmd, m.Key, m.Value, m.TTL)
		return []byte(cmd)
	case CMDGet:
		cmd := fmt.Sprintf("%s %s", m.Cmd, m.Key)
		return []byte(cmd)
	default:
		panic("unknown command")
	}

}

func parseMessage(raw []byte) (*Message, error) {
	var (
		rawStr = string(raw)
		parts = strings.Split(rawStr, " ")
	)
	if len(parts) < 2 {
		return nil, errors.New("invalid protocol format")
	}

	msg := &Message {
		Cmd: Command(parts[0]),
		Key: []byte(parts[1]),
	}

	if msg.Cmd == CMDSet {
		if len(parts) < 4 {
			return nil, errors.New("invalid SET command")
		}
		msg.Value = []byte(parts[2])
		ttl, err := strconv.Atoi(parts[3])
		if err != nil {
			log.Println("Invalid parameter type for TTL")
		}
		msg.TTL = time.Second * time.Duration(ttl)
	}
	return msg, nil
}
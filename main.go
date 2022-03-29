package main

import (
	"learn/pkg/redis_aof/persistence"
	"log"
)

func main() {
	persistence.ReadConfig("config.yaml")
	log.Print("=================================\n")
	clients, err := persistence.Open()
	if err != nil {
		log.Fatalf("open redis err: %s", err.Error())
		return
	}
	signal := make(chan struct{})
	finish := make(chan struct{})
	aof := persistence.AOF{Clients: clients,
		IsNext: signal,
		Finish: finish,
	}
	aof.CheckCanWriteAof()
	log.Print("==============finish================\n")
}

package main

import (
	"consistent"
	"fmt"
	"log"
)

func main() {
	fmt.Println("example new")
	exampleNew()
	fmt.Println("example add")
	exampleAdd()
}

func exampleNew() {
	c := consistent.New()
	c.Add("cacheA")
	c.Add("cacheB")
	c.Add("cacheC")
	users := []string{"tom", "tony", "jerry", "alice", "bob"}
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
}

func exampleAdd() {
	c := consistent.New()
	c.Add("cacheA")
	c.Add("cacheB")
	c.Add("cacheC")
	users := []string{"tom", "tony", "jerry", "alice", "bob"}
	fmt.Println("initial state [A, B, C]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	c.Add("cacheD")
	c.Add("cacheE")
	fmt.Println("\nwith cacheD, cacheE [A, B, C, D, E]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
}

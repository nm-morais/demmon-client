package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
)

var (
	port int
	host string
)

func main() {
	flag.StringVar(&host, "h", "", "The host of the demmon http service")
	flag.IntVar(&port, "p", 0, "The port of the demmon http service")
	flag.Parse()

	if port == 0 {
		fmt.Println("port not defined")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if host == "" {
		fmt.Println("host not defined")
		flag.PrintDefaults()
		os.Exit(1)
	}

	clConf := client.DemmonClientConf{
		RequestTimeout: 3 * time.Second,
		DemmonPort:     port,
		DemmonHostAddr: host,
	}
	Repl(clConf)
}

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	demmon_client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
)

type Operation int

const (
	nodeUpdatesOp Operation = iota
	metricsOp
	inViewOp
)

var operations = []string{
	"Node Updates",
	"get Registered Metrics",
	"Get InView",
}

func (o Operation) String() string {
	return operations[o]
}

func readOp(reader *bufio.Reader) (*Operation, []string, error) {
	fmt.Print("-> ")
	text, _ := reader.ReadString('\n')
	// convert CRLF to LF
	text = strings.Replace(text, "\n", "", -1)
	split := strings.Split(text, " ")
	if len(split) == 0 {
		return nil, nil, errors.New("invalid operation")
	}
	op, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		fmt.Println(err.Error())
	}
	opConverted := Operation(op)
	return &opConverted, split[1:], nil
}

func printOps(f io.Writer) {
	fmt.Fprintf(f, "Operations:\n")
	for idx, op := range operations {
		fmt.Fprintf(f, "%d) %+v\n", idx, op)
	}
}

func printNodeUpdates(nodeUps, nodeDowns chan body_types.NodeUpdates) {
	f := bufio.NewWriter(os.Stdout)
	for {
		select {
		case nodeUpdate := <-nodeUps:
			fmt.Fprintln(f, "NODE UP")
			fmt.Fprintln(f, "Node up: ", nodeUpdate.Node)
			fmt.Fprintln(f, "View: ", nodeUpdate.View)
		case nodeUpdate := <-nodeDowns:
			fmt.Fprintln(f, "NODE DOWN")
			fmt.Fprintln(f, "Node down: ", nodeUpdate.Node)
			fmt.Fprintln(f, "View: ", nodeUpdate.View)
		}
		f.Flush()
	}
}

func errFunc(err error) {
	panic(err)
}

func Repl(clientConf demmon_client.DemmonClientConf) {
	f := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Demmon Shell")
	// nodeUps, nodeDowns := demmon.GetPeerNotificationChans()
	// go printNodeUpdates(nodeUps, nodeDowns)
	c, err := demmon_client.New(clientConf, errFunc)
	if err != nil {
		panic(err)
	}
	err = c.ConnectTimeout(1 * time.Second)
	if err != nil {
		panic(err)
	}
	for {
		printOps(f)
		f.Flush()
		op, _, err := readOp(reader)
		if err != nil {
			fmt.Fprintf(f, "got err: %+s\n", err)
		}
		switch *op {
		case metricsOp:
			res, err := c.GetRegisteredMetrics()
			if err != nil {
				fmt.Fprintf(f, "got err: %+s\n", err)
			}
			fmt.Fprintf(f, "\n")
			for i, m := range res {
				fmt.Fprintf(f, "metric %d: %s\n", i, m)
			}
			fmt.Fprintf(f, "\n")
		case inViewOp:
			fmt.Fprintf(f, "%+v\n", c.GetInView())
		default:
			fmt.Fprintf(f, "No handler for operation: <%s>\n", op)
		}
		f.Flush()
	}
}

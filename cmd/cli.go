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

	demmonClient "github.com/nm-morais/demmon-client/pkg"
)

type Operation int

const (
	nodeUpdatesOp Operation = iota
	makeQuery
	metricsOp
	installContinuousQuery
	inViewOp
)

var operations = []string{
	"Node Updates",
	"Make Query",
	"get Registered Metrics",
	"Install Continuous Query",
	"Get InView",
}

func (o Operation) String() string {
	return operations[o]
}

func readOp(reader *bufio.Reader) (*Operation, string, error) {
	fmt.Print("-> ")
	text, _ := reader.ReadString('\n')
	// convert CRLF to LF
	text = strings.Replace(text, "\n", "", -1)
	split := strings.Split(text, " ")
	if len(split) == 0 {
		return nil, "", errors.New("invalid operation")
	}
	op, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		fmt.Println(err.Error())
	}
	opConverted := Operation(op)
	return &opConverted, strings.Join(split[1:], " "), nil
}

func printOps(f io.Writer) {
	_, _ = fmt.Fprintf(f, "Operations:\n")
	for idx, op := range operations {
		_, _ = fmt.Fprintf(f, "%d) %+v\n", idx, op)
	}
}

func Repl(clientConf demmonClient.DemmonClientConf) {
	f := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Demmon Shell")
	// nodeUps, nodeDowns := demmon.GetPeerNotificationChans()
	// go printNodeUpdates(nodeUps, nodeDowns)
	c := demmonClient.New(clientConf)
	err := c.ConnectTimeout(1 * time.Second)
	if err != nil {
		panic(err)
	}
	for {
		printOps(f)
		_ = f.Flush()
		op, args, err := readOp(reader)
		if err != nil {
			_, _ = fmt.Fprintf(f, "got err: %s\n", err)
		}
		switch *op {
		case metricsOp:
			res, err := c.GetRegisteredMetrics()
			if err != nil {
				_, _ = fmt.Fprintf(f, "got err: %s\n", err)
			}
			_, _ = fmt.Fprintf(f, "\n")
			for i, m := range res {
				_, _ = fmt.Fprintf(f, "metric %d: %s\n", i, m)
			}
			_, _ = fmt.Fprintf(f, "\n")
		case inViewOp:
			_, _ = fmt.Fprintf(f, "%+v\n", c.GetInView())
		case makeQuery:
			argsSplit := strings.Split(args, " ")
			if len(args) < 2 {
				_, _ = fmt.Fprintf(f, "Err: incorrect number of args, want: 2 got: %d\n", len(args))
				continue
			}
			queryTimeoutDuration, err := strconv.ParseInt(argsSplit[0], 10, 64)
			queryStr := strings.Join(argsSplit[1:], " ")
			fmt.Println(queryStr)
			if err != nil {
				_, _ = fmt.Fprintf(f, "got error: %s\n", err.Error())
				continue
			}
			res, err := c.Query(queryStr, time.Duration(queryTimeoutDuration)*time.Second)
			if err != nil {
				_, _ = fmt.Fprintf(f, "Got err: %s\n", err.Error())
				continue
			}
			_, _ = fmt.Fprintf(f, "\ngot %d series in response\n", len(res))
			for _, ts := range res {
				_, _ = fmt.Fprintf(f, "ts: %+v\n", ts)
			}
		case installContinuousQuery:
			argsSplit := strings.Split(args, " ")
			if len(args) < 4 {
				_, _ = fmt.Fprintf(f, "Err: incorrect number of args, want: 4 got: %d\n", len(args))
				continue
			}

			outputMetricName := argsSplit[0]
			queryTimeout, err := strconv.ParseInt(argsSplit[1], 10, 64)
			if err != nil {
				_, _ = fmt.Fprintf(f, "got error: %s\n", err.Error())
				continue
			}
			queryFrequencySeconds, err := strconv.ParseInt(argsSplit[2], 10, 64)
			if err != nil {
				_, _ = fmt.Fprintf(f, "got error: %s\n", err.Error())
				continue
			}
			outputMetricCount, err := strconv.ParseInt(argsSplit[3], 10, 64)
			if err != nil {
				_, _ = fmt.Fprintf(f, "got error: %s\n", err.Error())
				continue
			}

			queryStr := strings.Join(argsSplit[4:], " ")
			res, err := c.InstallContinuousQuery(
				queryStr,
				"",
				time.Duration(queryFrequencySeconds),
				time.Duration(queryTimeout)*time.Second,
				outputMetricName,
				int(outputMetricCount),
				3,
			)
			if err != nil {
				_, _ = fmt.Fprintf(f, "Got err: %s\n", err.Error())
				continue
			}
			_, _ = fmt.Fprintf(f, "\ngot response: %+v\n", res)
		default:
			_, _ = fmt.Fprintf(f, "No handler for operation: <%s>\n", op)
		}
		_= f.Flush()
	}
}

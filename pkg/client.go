package client

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

var (
	NotConnectedErr = errors.New("not connected")
)

// Call represents an active request
type Call struct {
	Req   body_types.Request
	Res   body_types.Response
	Done  chan bool
	Error error
}

// Call represents an active request
type Subscription struct {
	Res   chan body_types.Response
	Done  chan bool
	Error error
}

func newCall(req body_types.Request) *Call {
	return &Call{
		Req:  req,
		Done: make(chan bool),
	}
}

func newSub() *Subscription {
	return &Subscription{
		Res:  make(chan body_types.Response),
		Done: make(chan bool),
	}
}

type DemmonClientConf struct {
	DemmonPort     int
	DemmonHostAddr string
	RequestTimeout time.Duration
	ChunkSize      int
}

type DemmonClient struct {
	conf DemmonClientConf

	mutex   sync.Mutex
	conn    *websocket.Conn
	pending map[uint64]*Call
	subs    map[uint64]*Subscription
	counter uint64

	nodeUps   chan body_types.NodeUpdates
	nodeDowns chan body_types.NodeUpdates
}

func New(conf DemmonClientConf, connDownFunc func(error)) (*DemmonClient, error) {
	cl := &DemmonClient{
		conf:      conf,
		mutex:     sync.Mutex{},
		counter:   1,
		subs:      make(map[uint64]*Subscription),
		pending:   make(map[uint64]*Call),
		nodeUps:   make(chan body_types.NodeUpdates),
		nodeDowns: make(chan body_types.NodeUpdates),
	}
	return cl, nil
}

func (cl *DemmonClient) GetInView() []peer.Peer {
	return nil
}

func (cl *DemmonClient) GetRegisteredMetrics() ([]string, error) {
	resp, err := cl.request(routes.GetRegisteredMetrics, nil)
	if err != nil {
		return nil, err
	}
	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}
	res := resp.Message.([]string)
	return res, nil
}

func (cl *DemmonClient) RegisterMetrics(metrics []body_types.MetricMetadata) error {
	resp, err := cl.request(routes.RegisterMetrics, metrics)
	if err != nil {
		return err
	}
	if resp.Error {
		return resp.GetMsgAsErr()
	}
	return nil
}

func (cl *DemmonClient) PushMetricBlob(service, origin string, values []string) error {
	resp, err := cl.request(routes.PushMetricBlob, values)
	if err != nil {
		return err
	}
	if resp.Error {
		return resp.GetMsgAsErr()
	}
	return err
}

func (cl *DemmonClient) AddPlugin(pluginPath, pluginName string) error {
	file, err := os.Open(pluginPath) // For read access.
	if err != nil {
		return err
	}
	i := 0
	for {
		chunk := make([]byte, cl.conf.ChunkSize)
		n, err := file.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		contentb64 := base64.StdEncoding.EncodeToString(chunk[:n])
		toSend := body_types.PluginFileBlock{
			Name:       pluginName,
			FirstBlock: i == 0,
			FinalBlock: false,
			Content:    contentb64,
		}
		_, err = cl.request(routes.AddPlugin, toSend)
		if err != nil {
			return err
		}
		i++
	}
	toSend := body_types.PluginFileBlock{
		Name:       pluginName,
		FinalBlock: true,
		Content:    "",
	}
	_, err = cl.request(routes.AddPlugin, toSend)
	if err != nil {
		return err
	}
	return nil
}

func (cl *DemmonClient) GetRegisteredPlugins() ([]string, error) {
	resp, err := cl.request(routes.GetRegisteredPlugins, nil)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}
	return resp.Message.([]string), nil
}

func (cl *DemmonClient) BroadcastMessage(msg message.Message, nrHops int) {

}

func (cl *DemmonClient) RegisterBroadcastMessageHandler(msgId message.ID, handler func(message.Message)) {

}

func (cl *DemmonClient) QueryMetric(serviceName, metricName, origin, expression string) float64 {
	return 0
}

func (cl *DemmonClient) InstallTreeAggregationFunction(nrHops int, sourceMetricName, resultingMetricName, timeFrame, expression string) { // TODO queries

}

func (cl *DemmonClient) InstallLocalAggregationFunction(sourceMetricName, resultingMetricName, timeFrame, expression string) { // TODO queries

}

func (c *DemmonClient) ConnectTimeout(timeout time.Duration) error {
	u := url.URL{Host: fmt.Sprintf("%s:%d", c.conf.DemmonHostAddr, c.conf.DemmonPort), Path: routes.Dial, Scheme: "ws"}
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), http.Header{})
	if err != nil {
		return err
	}
	c.conn = conn
	go c.read()
	return nil
}

func (c *DemmonClient) request(reqType int, payload interface{}) (*body_types.Response, error) {
	if c.conn == nil {
		return nil, NotConnectedErr
	}
	c.mutex.Lock()
	id := c.counter
	c.counter++
	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	c.pending[id] = call
	err := c.conn.WriteJSON(&req)
	if err != nil {
		delete(c.subs, id)
		delete(c.pending, id)
		c.mutex.Unlock()
		return nil, err
	}
	c.mutex.Unlock()
	select {
	case <-call.Done:
	case <-time.After(2 * time.Second):
		call.Error = errors.New("request timeout")

	}
	if call.Error != nil {
		return nil, call.Error
	}
	return &call.Res, nil
}

func (c *DemmonClient) subscribe(reqType int, payload interface{}) (*body_types.Response, *Subscription, error) {
	if c.conn == nil {
		return nil, nil, NotConnectedErr
	}
	c.mutex.Lock()
	id := c.counter
	c.counter++
	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	c.pending[id] = call
	newSub := newSub()
	c.subs[id] = newSub
	err := c.conn.WriteJSON(&req)
	if err != nil {
		delete(c.subs, id)
		delete(c.pending, id)
		c.mutex.Unlock()
		return nil, nil, err
	}
	c.mutex.Unlock()
	select {
	case <-call.Done:
	case <-time.After(2 * time.Second):
		call.Error = errors.New("request timeout")
	}
	if call.Error != nil {
		return nil, nil, call.Error
	}
	return &call.Res, newSub, nil
}

func (c *DemmonClient) read() {
	var err error
	for err == nil {
		var res body_types.Response
		err = c.conn.ReadJSON(&res)
		if err != nil {
			err = fmt.Errorf("error reading message: %q", err)
			continue
		}
		if res.Push {
			c.mutex.Lock()
			sub := c.subs[res.ID]
			c.mutex.Unlock()
			if sub == nil {
				err = errors.New("no subscription found")
				continue
			}
			select {
			case sub.Res <- res:
			default:
				err = errors.New("could not deliver subscription result")
				continue
			}
		}
		// fmt.Printf("received message: %+v\n", res)
		call := c.pending[res.ID]
		c.mutex.Lock()
		delete(c.pending, res.ID)
		c.mutex.Unlock()
		if call == nil {
			err = errors.New("no pending request found")
			continue
		}

		if res.Error {
			call.Error = errors.New(res.Message.(string))
			call.Done <- true
		} else {
			call.Res = res
			call.Done <- true
		}

	}
	c.mutex.Lock()
	for _, call := range c.pending {
		call.Error = err
		call.Done <- true
	}
	c.mutex.Unlock()
}

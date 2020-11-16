package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"golang.org/x/net/context"
)

var (
	ErrNotConnected = errors.New("not connected")
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
	Done        chan interface{}
	ContentChan chan interface{}
	Id          uint64
}

func newCall(req body_types.Request) *Call {
	return &Call{
		Req:  req,
		Done: make(chan bool),
	}
}

func newSub(id uint64) *Subscription {
	return &Subscription{
		Id:          id,
		Done:        make(chan interface{}),
		ContentChan: make(chan interface{}),
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
	resp, err := cl.request(routes.GetRegisteredMetricBuckets, nil)
	if err != nil {
		return nil, err
	}
	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := []string{}
	err = mapstructure.Decode(resp.Message, &respDecoded)
	if resp.Error {
		return nil, err
	}
	return respDecoded, nil
}

func (cl *DemmonClient) PushMetricBlob(values body_types.PointCollection) error {
	resp, err := cl.request(routes.PushMetricBlob, values)
	if err != nil {
		return err
	}
	if resp.Error {
		return resp.GetMsgAsErr()
	}
	return err
}

func (cl *DemmonClient) BroadcastMessage(msg message.Message, nrHops int) {

}

func (cl *DemmonClient) RegisterBroadcastMessageHandler(msgId message.ID, handler func(message.Message)) {

}

func (cl *DemmonClient) MakeQuery(serviceName, metricName, origin, expression string) float64 {
	return 0
}

func (cl *DemmonClient) SubscribeQueryPeriodic() error {
	return nil
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
	return err

}

func (c *DemmonClient) request(reqType routes.RequestType, payload interface{}) (*body_types.Response, error) {
	if c.conn == nil {
		return nil, ErrNotConnected
	}
	c.mutex.Lock()
	id := c.counter
	c.counter++
	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	c.pending[id] = call
	err := c.conn.WriteJSON(&req)
	if err != nil {
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

func (c *DemmonClient) subscribe(reqType routes.RequestType, payload interface{}) (*body_types.Response, *Subscription, error) {
	if c.conn == nil {
		return nil, nil, ErrNotConnected
	}
	c.mutex.Lock()
	id := c.counter
	c.counter++
	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	c.pending[id] = call
	newSub := newSub(id)
	c.subs[id] = newSub
	err := c.conn.WriteJSON(&req)
	if err != nil {
		close(newSub.Done)
		close(newSub.ContentChan)
		delete(c.subs, id)
		delete(c.pending, id)
		c.mutex.Unlock()
		return nil, nil, err
	}
	c.mutex.Unlock()
	select {
	case <-call.Done:
	case <-time.After(2 * time.Second):
		c.mutex.Lock()
		delete(c.subs, id)
		c.mutex.Unlock()
		call.Error = errors.New("request timeout")
		return nil, nil, call.Error
	}
	return &call.Res, newSub, nil
}

func (c *DemmonClient) clearSub(sub *Subscription) {
	fmt.Println("Clearing subscription...")
	c.mutex.Lock()
	delete(c.subs, sub.Id)
	c.mutex.Unlock()
	close(sub.Done)
	close(sub.ContentChan)
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
			fmt.Printf("Got push: %+v\n", res)
			c.mutex.Lock()
			sub := c.subs[res.ID]
			c.mutex.Unlock()
			if sub == nil {
				fmt.Println(errors.New("no subscription found"))
				continue
			}
			select {
			case <-sub.Done:
				fmt.Println(errors.New("could not deliver subscription result because it finished already"))
			case sub.ContentChan <- res.Message:
				fmt.Println("Delivered content to sub")
			case <-time.After(1 * time.Second):
				err = errors.New("could not deliver subscription result because there was no listener")
			}
			continue
		}
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
	fmt.Println("Read routine exiting due to err: ", err)
	c.mutex.Lock()
	for _, call := range c.pending {
		call.Error = err
		call.Done <- true
	}
	c.mutex.Unlock()
}

// func (cl *DemmonClient) RegisterMetrics(metrics []body_types.MetricMetadata) error {
// 	resp, err := cl.request(routes.RegisterMetrics, metrics)
// 	if err != nil {
// 		return err
// 	}
// 	if resp.Error {
// 		return resp.GetMsgAsErr()
// 	}
// 	return nil
// }

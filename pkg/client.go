package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

var (
	ErrNotConnected = errors.New("not connected")
)

type QuerySubscription struct {
	ResChan chan *[]body_types.Timeseries
	ErrChan chan error
}

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

func New(conf DemmonClientConf) *DemmonClient {
	cl := &DemmonClient{
		conf:      conf,
		mutex:     sync.Mutex{},
		counter:   1,
		subs:      make(map[uint64]*Subscription),
		pending:   make(map[uint64]*Call),
		nodeUps:   make(chan body_types.NodeUpdates),
		nodeDowns: make(chan body_types.NodeUpdates),
	}
	return cl
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
	err = decode(resp.Message, &respDecoded)
	if resp.Error {
		return nil, err
	}
	return respDecoded, nil
}

func (cl *DemmonClient) PushMetricBlob(values body_types.PointCollectionWithTagsAndName) error {
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

func (cl *DemmonClient) SubscribeQuery(expression string, timeout, repeatTime time.Duration) QuerySubscription {
	reqBody := body_types.QueryRequest{
		Query: body_types.RunnableExpression{
			Timeout:    timeout,
			Expression: expression,
		},
	}
	querySub := QuerySubscription{
		ResChan: make(chan *[]body_types.Timeseries),
		ErrChan: make(chan error),
	}
	go func() {
		defer close(querySub.ErrChan)
		defer close(querySub.ResChan)
		for {
			resp, err := cl.request(routes.Query, reqBody)
			if err != nil {
				querySub.ErrChan <- err
			}
			if resp.Error {
				querySub.ErrChan <- resp.GetMsgAsErr()
			}

			respDecoded := []*body_types.Timeseries{}
			err = decode(resp.Message, &respDecoded)
			if resp.Error {
				querySub.ErrChan <- err
			}
		}
	}()
	return querySub
}

func (cl *DemmonClient) Query(expression string, timeout time.Duration) ([]body_types.Timeseries, error) {
	reqBody := body_types.QueryRequest{
		Query: body_types.RunnableExpression{
			Expression: expression,
			Timeout:    timeout,
		},
	}
	resp, err := cl.request(routes.Query, reqBody)
	if err != nil {
		return nil, err
	}
	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}
	respDecoded := []body_types.Timeseries{}
	err = decode(resp.Message, &respDecoded)
	if err != nil {
		return nil, err
	}
	return respDecoded, nil
}

func (cl *DemmonClient) InstallContinuousQuery(
	expression string,
	description string,
	expressionTimeout time.Duration,
	outputMetricFrequency time.Duration,
	outputMetricName string,
	outputMetricCount int,
	nrRetries int,
) (uint64, error) {
	reqBody := body_types.InstallContinuousQueryRequest{
		Expression:        expression,
		Description:       description,
		ExpressionTimeout: expressionTimeout,
		NrRetries:         nrRetries,
		OutputBucketOpts: body_types.BucketOptions{
			Name: outputMetricName,
			Granularity: body_types.Granularity{
				Granularity: outputMetricFrequency,
				Count:       outputMetricCount,
			},
		},
	}

	resp, err := cl.request(routes.InstallContinuousQuery, reqBody)
	if err != nil {
		return math.MaxUint64, err
	}
	if resp.Error {
		return math.MaxUint64, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallContinuousQueryReply{}
	err = decode(resp.Message, &respDecoded)
	if err != nil {
		return math.MaxUint64, err
	}
	if resp.Error {
		return math.MaxUint64, err
	}
	return respDecoded.TaskId, nil
}

func (cl *DemmonClient) GetContinuousQueries() (*body_types.GetContinuousQueriesReply, error) {
	resp, err := cl.request(routes.GetContinuousQueries, nil)
	if err != nil {
		return nil, err
	}
	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := &body_types.GetContinuousQueriesReply{}
	err = decode(resp.Message, &respDecoded)
	if err != nil {
		return nil, err
	}
	if resp.Error {
		return nil, err
	}
	return respDecoded, nil
}

func (cl *DemmonClient) InstallCustomInterestSet(expression string, expressionTimeout time.Duration, outputMetricName string, hosts []*body_types.Peer, outputMetricGranularity body_types.Granularity) (uint64, error) {
	set := body_types.CustomInterestSet{
		Query: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: expression},
		Hosts: hosts,
		OutputBucketOpts: body_types.BucketOptions{
			Name:        outputMetricName,
			Granularity: outputMetricGranularity,
		},
	}
	resp, err := cl.request(routes.InstallCustomInterestSet, set)
	if err != nil {
		return math.MaxUint64, err
	}
	if resp.Error {
		return math.MaxUint64, resp.GetMsgAsErr()
	}
	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)
	if resp.Error {
		return math.MaxUint64, err
	}

	return respDecoded.SetId, nil
}

func (cl *DemmonClient) InstallNeighborhoodInterestSet(expression string, expressionTimeout time.Duration, ttl int, outputMetricName string, runFrequency time.Duration, outputMetricStorageCount, maxQueryRetries int) (uint64, error) {
	set := body_types.NeighborhoodInterestSet{
		MaxRetries: maxQueryRetries,
		Query:      body_types.RunnableExpression{Timeout: expressionTimeout, Expression: expression},
		TTL:        ttl,
		OutputBucketOpts: body_types.BucketOptions{
			Name: outputMetricName,
			Granularity: body_types.Granularity{
				Granularity: runFrequency,
				Count:       outputMetricStorageCount,
			},
		},
	}
	resp, err := cl.request(routes.InstallNeighborhoodInterestSet, set)
	if err != nil {
		return math.MaxUint64, err
	}
	if resp.Error {
		return math.MaxUint64, resp.GetMsgAsErr()
	}
	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)
	if resp.Error {
		return math.MaxUint64, err
	}
	return respDecoded.SetId, nil
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

func (cl *DemmonClient) request(reqType routes.RequestType, payload interface{}) (*body_types.Response, error) {
	if cl.conn == nil {
		return nil, ErrNotConnected
	}
	cl.mutex.Lock()
	id := cl.counter
	cl.counter++
	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	cl.pending[id] = call
	err := cl.conn.WriteJSON(&req)
	if err != nil {
		delete(cl.pending, id)
		cl.mutex.Unlock()
		return nil, err
	}
	cl.mutex.Unlock()
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

func (cl *DemmonClient) subscribe(reqType routes.RequestType, payload interface{}) (*body_types.Response, *Subscription, error) {
	if cl.conn == nil {
		return nil, nil, ErrNotConnected
	}
	cl.mutex.Lock()
	id := cl.counter
	cl.counter++
	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	cl.pending[id] = call
	newSub := newSub(id)
	cl.subs[id] = newSub
	err := cl.conn.WriteJSON(&req)
	if err != nil {
		close(newSub.Done)
		close(newSub.ContentChan)
		delete(cl.subs, id)
		delete(cl.pending, id)
		cl.mutex.Unlock()
		return nil, nil, err
	}
	cl.mutex.Unlock()
	select {
	case <-call.Done:
	case <-time.After(2 * time.Second):
		cl.mutex.Lock()
		delete(cl.subs, id)
		cl.mutex.Unlock()
		call.Error = errors.New("request timeout")
		return nil, nil, call.Error
	}
	return &call.Res, newSub, nil
}

func (cl *DemmonClient) clearSub(sub *Subscription) {
	fmt.Println("Clearing subscription...")
	cl.mutex.Lock()
	delete(cl.subs, sub.Id)
	cl.mutex.Unlock()
	close(sub.Done)
	close(sub.ContentChan)
}

func (cl *DemmonClient) read() {
	var err error
	for err == nil {
		var res body_types.Response
		err = cl.conn.ReadJSON(&res)
		if err != nil {
			err = fmt.Errorf("error reading message: %q", err)
			continue
		}
		if res.Push {
			fmt.Printf("Got push: %+v\n", res)
			cl.mutex.Lock()
			sub := cl.subs[res.ID]
			cl.mutex.Unlock()
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
		call := cl.pending[res.ID]
		cl.mutex.Lock()
		delete(cl.pending, res.ID)
		cl.mutex.Unlock()
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
	cl.mutex.Lock()
	for _, call := range cl.pending {
		call.Error = err
		call.Done <- true
	}
	cl.mutex.Unlock()
}

func decode(input interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata: nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			toTimeHookFunc()),
		Result: result,
	})
	if err != nil {
		return err
	}

	if err := decoder.Decode(input); err != nil {
		return err
	}
	return err
}

func toTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			return time.Parse(time.RFC3339, data.(string))
		case reflect.Float64:
			return time.Unix(0, int64(data.(float64))*int64(time.Millisecond)), nil
		case reflect.Int64:
			return time.Unix(0, data.(int64)*int64(time.Millisecond)), nil
		default:
			return data, nil
		}
		// Convert it by parsing
	}
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

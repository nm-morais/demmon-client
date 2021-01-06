package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
)

var (
	ErrNotConnected         = errors.New("not connected")
	ErrTimeout              = errors.New("request timed out")
	ErrSubFinished          = errors.New("subscription finished")
	ErrBadUnmarshal         = errors.New("an error occurred unmarshaling")
	ErrSubscriptionNotFound = errors.New("no subscription found")
)

type QuerySubscription struct {
	ResChan chan []body_types.TimeseriesDTO
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

func (cl *DemmonClient) GetInView() (*body_types.View, error) {
	resp, err := cl.request(routes.GetInView, nil)
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := &body_types.View{}
	err = decode(resp.Message, respDecoded)

	if resp.Error {
		return nil, err
	}

	return respDecoded, nil
}

func (cl *DemmonClient) SubscribeNodeUpdates() (*body_types.View, error, chan body_types.NodeUpdates) {

	resp, sub, err := cl.subscribe(routes.MembershipUpdates, nil)
	if err != nil {
		return nil, err, nil
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr(), nil
	}

	respDecoded := &body_types.View{}
	err = decode(resp.Message, respDecoded)

	if resp.Error {
		return nil, err, nil
	}

	nodeUpdateChan := make(chan body_types.NodeUpdates, 1)

	go func() {
		for v := range sub.ContentChan {
			update := body_types.NodeUpdates{}
			err = decode(v, &update)

			if err != nil {
				panic(err) // TODO error handling
			}
			nodeUpdateChan <- update
		}
	}()

	return respDecoded, err, nodeUpdateChan
}

func (cl *DemmonClient) GetRegisteredMetrics() ([]string, error) {
	resp, err := cl.request(routes.GetRegisteredMetricBuckets, nil)
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := make([]string, 0)
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, err
	}

	return respDecoded, nil
}

func (cl *DemmonClient) PushMetricBlob(values []body_types.TimeseriesDTO) error {
	resp, err := cl.request(routes.PushMetricBlob, values)
	if err != nil {
		return err
	}

	if resp.Error {
		return resp.GetMsgAsErr()
	}

	return err
}

func (cl *DemmonClient) SubscribeQuery(expression string, timeout, repeatTime time.Duration) QuerySubscription {
	reqBody := body_types.QueryRequest{
		Query: body_types.RunnableExpression{
			Timeout:    timeout,
			Expression: expression,
		},
	}
	querySub := QuerySubscription{
		ResChan: make(chan []body_types.TimeseriesDTO),
		ErrChan: make(chan error),
	}
	go func() {
		defer close(querySub.ErrChan)
		defer close(querySub.ResChan)

		for {
			resp, err := cl.request(routes.Query, reqBody)
			if err != nil {
				querySub.ErrChan <- err
				return
			}

			if resp.Error {
				querySub.ErrChan <- resp.GetMsgAsErr()
				return
			}

			respDecoded := make([]body_types.TimeseriesDTO, 0)
			err = decode(resp.Message, &respDecoded)
			if resp.Error {
				querySub.ErrChan <- err
				return
			} else {
				querySub.ResChan <- respDecoded
			}
		}
	}()

	return querySub
}

func (cl *DemmonClient) Query(expression string, timeout time.Duration) ([]body_types.TimeseriesDTO, error) {
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

	respDecoded := []body_types.TimeseriesDTO{}
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
	return respDecoded.TaskID, nil
}

func (cl *DemmonClient) InstallBucket(name string, frequency time.Duration, sampleCount int) error {
	reqBody := body_types.BucketOptions{
		Name:        name,
		Granularity: body_types.Granularity{Granularity: frequency, Count: sampleCount},
	}
	resp, err := cl.request(routes.InstallBucket, reqBody)
	if err != nil {
		return err
	}

	if resp.Error {
		return resp.GetMsgAsErr()
	}
	return nil
}

func (cl *DemmonClient) InstallBroadcastMessageHandler(messageID uint64) (chan body_types.Message, error) {
	reqBody := body_types.InstallMessageHandlerRequest{
		ID: messageID,
	}

	resp, sub, err := cl.subscribe(routes.InstallBroadcastMessageHandler, reqBody)
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	msgChan := make(chan body_types.Message, 1)
	go func() {
		for v := range sub.ContentChan {
			update := body_types.Message{}
			err = decode(v, &update)

			if err != nil {
				panic(err) // TODO error handling
			}
			msgChan <- update
		}
	}()
	return msgChan, err
}

func (cl *DemmonClient) BroadcastMessage(reqBody body_types.Message) error {
	resp, err := cl.request(routes.BroadcastMessage, reqBody)

	if err != nil {
		return err
	}

	if resp.Error {
		return resp.GetMsgAsErr()
	}

	return nil
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

func (cl *DemmonClient) InstallCustomInterestSet(
	expression string,
	expressionTimeout time.Duration,
	outputMetricName string,
	hosts []net.IP,
	outputMetricGranularity body_types.Granularity,
) (uint64, chan error, error) {
	set := body_types.CustomInterestSet{
		IS: body_types.InterestSet{
			Query: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: expression},
			OutputBucketOpts: body_types.BucketOptions{
				Name:        outputMetricName,
				Granularity: outputMetricGranularity,
			},
		},
		Hosts: hosts,
	}
	resp, sub, err := cl.subscribe(routes.InstallCustomInterestSet, set)
	if err != nil {
		return math.MaxUint64, nil, err
	}

	if resp.Error {
		return math.MaxUint64, nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return math.MaxUint64, nil, err
	}

	errChan := make(chan error, 1)

	go func() {
		for v := range sub.ContentChan {
			errMsg := ""
			err = decode(v, &errMsg)
			if err != nil {
				panic(err)
			}
			errChan <- err
			return
		}
	}()

	return respDecoded.SetID, errChan, nil
}

func (cl *DemmonClient) InstallNeighborhoodInterestSet(is *body_types.NeighborhoodInterestSet) (uint64, error) {
	resp, err := cl.request(routes.InstallNeighborhoodInterestSet, is)
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
	return respDecoded.SetID, nil
}

func (cl *DemmonClient) InstallTreeAggregationFunction(is *body_types.TreeAggregationSet) (uint64, error) {
	resp, err := cl.request(routes.InstallTreeAggregationFunction, is)
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

	return respDecoded.SetID, nil
}

func (cl *DemmonClient) ConnectTimeout(timeout time.Duration) error {
	u := url.URL{
		Host:   fmt.Sprintf("%s:%d", cl.conf.DemmonHostAddr, cl.conf.DemmonPort),
		Path:   routes.Dial,
		Scheme: "ws",
	}
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)

	defer cancel()

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, u.String(), http.Header{})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	cl.conn = conn
	go cl.read()
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
	case <-time.After(cl.conf.RequestTimeout):
		call.Error = ErrTimeout
	}

	if call.Error != nil {
		return nil, call.Error
	}

	return &call.Res, nil
}

func (cl *DemmonClient) subscribe(reqType routes.RequestType, payload interface{}) (
	*body_types.Response,
	*Subscription,
	error,
) {
	if cl.conn == nil {
		return nil, nil, ErrNotConnected
	}

	cl.mutex.Lock()
	id := cl.counter
	cl.counter++

	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	newSub := newSub(id)
	cl.pending[id] = call
	cl.subs[id] = newSub
	err := cl.conn.WriteJSON(&req)
	if err != nil {
		close(newSub.ContentChan)
		delete(cl.subs, id)
		delete(cl.pending, id)
		cl.mutex.Unlock()
		return nil, nil, err
	}
	cl.mutex.Unlock()
	select {
	case <-call.Done:
	case <-time.After(cl.conf.RequestTimeout):
		cl.mutex.Lock()
		delete(cl.pending, id)
		delete(cl.subs, id)
		cl.mutex.Unlock()
		call.Error = ErrTimeout
		return nil, nil, call.Error
	}
	return &call.Res, newSub, nil
}

func (cl *DemmonClient) clearSub(sub *Subscription) {
	fmt.Println("Clearing subscription...")
	cl.mutex.Lock()
	delete(cl.subs, sub.Id)
	close(sub.ContentChan)
	cl.mutex.Unlock()
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
				panic(ErrSubscriptionNotFound) // TODO remove
				fmt.Println(ErrSubscriptionNotFound)
				continue
			}
			select {
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
		select {
		case call.Done <- true:
		case <-time.After(1 * time.Second):
			panic("Timed out propagating error to call") // TODO remove
		}
	}
	cl.mutex.Unlock()
}

func decode(input, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(
		&mapstructure.DecoderConfig{
			TagName:  "json",
			Metadata: nil,
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				toTimeHookFunc(),
				mapstructure.StringToIPHookFunc(),
			),
			Result: result,
		},
	)
	if err != nil {
		return err
	}

	err = decoder.Decode(input)
	if err != nil {
		return err
	}

	return err
}

func toTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
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

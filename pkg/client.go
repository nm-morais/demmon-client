package client

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	ErrNoListener = errors.New("could not deliver subscription result because there was no listener")
)

var (
	ErrNotConnected         = errors.New("not connected")
	ErrTimeout              = errors.New("request timed out")
	ErrSubFinished          = errors.New("subscription finished")
	ErrBadUnmarshal         = errors.New("an error occurred unmarshaling")
	ErrSubscriptionNotFound = errors.New("no subscription found")
)

type QuerySubscription struct {
	FinishChan chan interface{}
	ResChan    chan []body_types.TimeseriesDTO
	ErrChan    chan error
}

// Call represents an active request
type Call struct {
	Req   body_types.Request
	Res   body_types.Response
	Done  chan interface{}
	Error error
}

// Call represents an active request
type Subscription struct {
	FinishChan  chan interface{}
	ContentChan chan interface{}
	Id          string
}

func newCall(req body_types.Request) *Call {
	return &Call{
		Req:  req,
		Done: make(chan interface{}),
	}
}

func newSub(id string) *Subscription {
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
	pending map[string]*Call
	subs    map[string]*Subscription
	counter uint64

	nodeUps   chan body_types.NodeUpdates
	nodeDowns chan body_types.NodeUpdates
	sync.Mutex
}

func New(conf DemmonClientConf) *DemmonClient {
	cl := &DemmonClient{
		conf:      conf,
		mutex:     sync.Mutex{},
		counter:   1,
		subs:      make(map[string]*Subscription),
		pending:   make(map[string]*Call),
		nodeUps:   make(chan body_types.NodeUpdates),
		nodeDowns: make(chan body_types.NodeUpdates),
		Mutex:     sync.Mutex{},
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

func (cl *DemmonClient) SubscribeNodeUpdates() (*body_types.View, error, chan interface{}, chan body_types.NodeUpdates) {

	resp, sub, err := cl.subscribe(routes.MembershipUpdates, nil)
	if err != nil {
		return nil, err, nil, nil
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr(), nil, nil
	}

	respDecoded := &body_types.View{}
	err = decode(resp.Message, respDecoded)

	if resp.Error {
		return nil, err, nil, nil
	}

	nodeUpdateChan := make(chan body_types.NodeUpdates)

	go func() {
		updates := []body_types.NodeUpdates{}

		handleUpdateFunc := func(nextUpdate interface{}) {
			update := body_types.NodeUpdates{}
			err = decode(nextUpdate, &update)
			if err != nil {
				panic(err)
			}
			updates = append(updates, update)
		}

		for {
			if len(updates) == 0 {
				nextUpdate := <-sub.ContentChan
				handleUpdateFunc(nextUpdate)
			}
			select {
			case v := <-sub.ContentChan:
				handleUpdateFunc(v)
			case nodeUpdateChan <- updates[0]:
				updates = updates[1:]
			case <-sub.FinishChan:
				updates = nil
				return
			}
		}
	}()

	return respDecoded, err, sub.FinishChan, nodeUpdateChan
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
		FinishChan: make(chan interface{}),
		ResChan:    make(chan []body_types.TimeseriesDTO),
		ErrChan:    make(chan error),
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
				select {
				case querySub.ErrChan <- resp.GetMsgAsErr():
				default:
				}
				return
			}

			respDecoded := make([]body_types.TimeseriesDTO, 0)
			err = decode(resp.Message, &respDecoded)
			if resp.Error {
				querySub.ErrChan <- err
				return
			}

			select {
			case querySub.ResChan <- respDecoded:
			case <-querySub.FinishChan:
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
) (*string, error) {
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
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallContinuousQueryReply{}
	err = decode(resp.Message, &respDecoded)

	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, err
	}

	return &respDecoded.TaskID, nil
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

func (cl *DemmonClient) InstallBroadcastMessageHandler(messageID string) (chan body_types.Message, chan interface{}, error) {
	reqBody := body_types.InstallMessageHandlerRequest{
		ID: messageID,
	}

	resp, sub, err := cl.subscribe(routes.InstallBroadcastMessageHandler, reqBody)
	if err != nil {
		return nil, nil, err
	}

	if resp.Error {
		return nil, nil, resp.GetMsgAsErr()
	}

	msgChan := make(chan body_types.Message)

	go func() {
		updates := []body_types.Message{}

		handleUpdateFunc := func(nextUpdate interface{}) {
			update := &body_types.Message{}
			err = decode(nextUpdate, &update)
			if err != nil {
				panic(err)
			}
			updates = append(updates, *update)
		}

		for {
			if len(updates) == 0 {
				nextUpdate := <-sub.ContentChan
				handleUpdateFunc(nextUpdate)
			}

			select {
			case v := <-sub.ContentChan:
				handleUpdateFunc(v)
			case msgChan <- updates[0]:
				updates = updates[1:]
			case <-sub.FinishChan:
				updates = nil
				return
			}
		}
	}()
	return msgChan, sub.FinishChan, err
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

func (cl *DemmonClient) InstallCustomInterestSet(set body_types.CustomInterestSet) (*string, chan error, chan interface{}, error) {
	resp, sub, err := cl.subscribe(routes.InstallCustomInterestSet, set)
	if err != nil {
		return nil, nil, nil, err
	}

	if resp.Error {
		return nil, nil, nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, nil, nil, err
	}

	errChan := make(chan error)

	go func() {
		for {
			select {
			case v := <-sub.ContentChan:
				body := body_types.CustomInterestSetErr{}
				err = decode(v, &body)
				if err != nil {
					panic(err)
				}
				select {
				case errChan <- errors.New(body.Err):

				case <-sub.FinishChan:
					return
				}
			case <-sub.FinishChan:
				return
			}
		}
	}()

	return &respDecoded.SetID, errChan, sub.FinishChan, err
}

func (cl *DemmonClient) InstallNeighborhoodInterestSet(is *body_types.NeighborhoodInterestSet) (*string, error) {
	resp, err := cl.request(routes.InstallNeighborhoodInterestSet, is)
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, err
	}
	return &respDecoded.SetID, nil
}

func (cl *DemmonClient) StartBabel() error {
	resp, err := cl.request(routes.StartBabel, nil)
	if err != nil {
		return err
	}

	if resp.Error {
		return resp.GetMsgAsErr()
	}
	return nil
}

func (cl *DemmonClient) InstallGlobalAggregationFunction(is *body_types.GlobalAggregationFunction) (*string, error) {
	resp, err := cl.request(routes.InstallGlobalAggregationFunction, is)
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, err
	}
	return &respDecoded.SetID, nil
}

func (cl *DemmonClient) InstallTreeAggregationFunction(is *body_types.TreeAggregationSet) (*string, error) {
	resp, err := cl.request(routes.InstallTreeAggregationFunction, is)
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallInterestSetReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, err
	}

	return &respDecoded.SetID, nil
}

func (cl *DemmonClient) RemoveCustomInterestSet(id string) (*string, error) {
	resp, err := cl.request(routes.RemoveCustomInterestSet, body_types.RemoveResourceRequest{ResourceID: id})
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.RemoveResourceReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, err
	}

	return &respDecoded.ResourceID, nil
}

func (cl *DemmonClient) RemoveAlarm(id string) (*string, error) {
	resp, err := cl.request(routes.RemoveCustomInterestSet, body_types.RemoveResourceRequest{ResourceID: id})
	if err != nil {
		return nil, err
	}

	if resp.Error {
		return nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.RemoveResourceReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, err
	}

	return &respDecoded.ResourceID, nil
}

func (cl *DemmonClient) UpdateCustomInterestSet(updateReq body_types.UpdateCustomInterestSetReq) error {
	resp, err := cl.request(routes.UpdateCustomInterestSetHosts, updateReq)
	if err != nil {
		return err
	}

	if resp.Error {
		return resp.GetMsgAsErr()
	}

	return nil
}

func (cl *DemmonClient) InstallAlarm(alarm *body_types.InstallAlarmRequest) (*string, chan bool, chan error, chan interface{}, error) {

	resp, sub, err := cl.subscribe(routes.InstallAlarm, alarm)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if resp.Error {
		return nil, nil, nil, nil, resp.GetMsgAsErr()
	}

	respDecoded := body_types.InstallAlarmReply{}
	err = decode(resp.Message, &respDecoded)

	if resp.Error {
		return nil, nil, nil, nil, err
	}

	alarmTriggerChan := make(chan bool)
	alarmErrorChan := make(chan error)

	go func() {
		updates := []body_types.AlarmUpdate{}
		handleUpdateFunc := func(nextUpdate interface{}) {
			update := body_types.AlarmUpdate{}
			err = decode(nextUpdate, &update)

			if err != nil {
				panic(err)
			}
			updates = append(updates, update)
		}

		for {
			if len(updates) == 0 {
				nextUpdate := <-sub.ContentChan
				handleUpdateFunc(nextUpdate)
			}

			if updates[0].Error {
				select {
				case <-sub.FinishChan:
					return
				case alarmErrorChan <- fmt.Errorf("%s", updates[0].ErrorMsg):
				}
				return
			}

			select {
			case v := <-sub.ContentChan:
				handleUpdateFunc(v)
			case alarmTriggerChan <- updates[0].Trigger:
				updates = updates[1:]
			case <-sub.FinishChan:
				return
			}
		}
	}()

	return &respDecoded.ID, alarmTriggerChan, alarmErrorChan, sub.FinishChan, err
}

func (cl *DemmonClient) ConnectTimeout(timeout time.Duration) (error, chan error) {
	u := url.URL{
		Host:   fmt.Sprintf("%s:%d", cl.conf.DemmonHostAddr, cl.conf.DemmonPort),
		Path:   routes.Dial,
		Scheme: "ws",
	}
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)

	defer cancel()

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, u.String(), http.Header{})
	if err != nil {
		return err, nil
	}
	defer resp.Body.Close()

	connErrChan := make(chan error)
	cl.conn = conn
	go cl.read(connErrChan)
	return err, connErrChan
}

func (cl *DemmonClient) request(reqType routes.RequestType, payload interface{}) (*body_types.Response, error) {
	if cl.conn == nil {
		return nil, ErrNotConnected
	}

	cl.mutex.Lock()
	id := fmt.Sprintf("%d", cl.counter)
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
	id := fmt.Sprintf("%d", cl.counter)
	cl.counter++

	req := body_types.Request{ID: id, Type: reqType, Message: payload}
	call := newCall(req)
	newSub := newSub(id)
	cl.subs[id] = newSub
	cl.pending[id] = call
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

	if !call.Res.Error {
		go func() {
			<-newSub.FinishChan
			cl.clearSub(newSub)
		}()
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

func (cl *DemmonClient) read(errChan chan error) {
	var err error
	for err == nil {
		var res body_types.Response
		err = cl.conn.ReadJSON(&res)
		if err != nil {
			break
		}

		if res.Push {
			cl.mutex.Lock()
			sub := cl.subs[res.ID]
			cl.mutex.Unlock()

			if sub == nil {
				fmt.Println(ErrSubscriptionNotFound)
				continue
			}

			select {
			case <-sub.FinishChan:
			default:
				select {
				case sub.ContentChan <- res.Message:
				case <-sub.FinishChan:
				case <-time.After(3 * time.Second):
					err = ErrNoListener
				}
			}

			continue
		}

		cl.mutex.Lock()
		call := cl.pending[res.ID]
		delete(cl.pending, res.ID)

		if call == nil {
			for k, v := range cl.pending {
				fmt.Printf("Pending call: %s %+v\n", k, v)
			}
			panic(fmt.Sprintf("no pending request found for request %+v", res))
		}
		cl.mutex.Unlock()

		if res.Error {
			call.Error = errors.New(res.Message.(string))
			close(call.Done)
		} else {
			call.Res = res
			close(call.Done)
		}
	}

	fmt.Printf("Got err reading: %s\n", err.Error())
	cl.mutex.Lock()
	cl.conn = nil
	cl.mutex.Unlock()
	select {
	case errChan <- err:
	case <-time.After(time.Second):
	}

	// TODO should cleanup pending calls ??
	// fmt.Println("Read routine exiting due to err: ", err)
	// cl.mutex.Lock()
	// for _, call := range cl.pending {
	// 	call.Error = err
	// 	close(call.Done)
	// 	select {
	// 	case <-call.Done:
	// 		delete(cl.pending, res.ID)
	// 	case <-time.After(1 * time.Second):
	// 		panic("Timed out propagating error to call") // TODO remove
	// 	}
	// }
	// cl.mutex.Unlock()
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
	}
}

func (cl *DemmonClient) Disconnect() {
	err := cl.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "disconnecting"), time.Now().Add(time.Second))
	if err != nil && errors.Is(err, websocket.ErrCloseSent) {
		log.Println("write error writing close message:", err)
		cl.conn.Close()
		return
	}

	go func() {
		<-time.After(time.Second)
		cl.mutex.Lock()
		if cl.conn != nil {
			cl.conn.Close()
		}
		cl.mutex.Unlock()
	}()
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

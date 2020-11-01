package demmon_client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	"github.com/nm-morais/demmon-common/timeseries"
	"github.com/nm-morais/demmon/internal/membership"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const (
	ContentTypeJSON = "application/json"
	ContentTypeText = "text/plain; charset=utf-8"
)

type DemmonClientConf struct {
	DemmonPort      int
	DemmonHostAddr  string
	ReqTimeout      time.Duration
	LocalListenPort int
}

type NodeUpdates struct {
	Node *membership.PeerWithIdChain
	View []*membership.PeerWithIdChain
}

type Client struct {
	client    *http.Client
	conf      DemmonClientConf
	nodeUps   chan NodeUpdates
	nodeDowns chan NodeUpdates
}

func New(conf DemmonClientConf) *Client {
	return &Client{
		client: &http.Client{
			Timeout: conf.ReqTimeout,
		},
		conf:      conf,
		nodeDowns: make(chan NodeUpdates),
		nodeUps:   make(chan NodeUpdates),
	}

}

func (cl *Client) GetInView() []*membership.PeerWithIdChain {
	return nil
}

func (cl *Client) GetRegisteredMetrics() ([]string, error) {
	resp, err := cl.makeRequestNoBody(routes.GetMetricsMethod, routes.GetMetricsPath)
	if err != nil {
		return nil, err
	}

	res := []string{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (cl *Client) RegisterMetrics(metrics []body_types.MetricMetadata) error {
	toSend, err := json.Marshal(metrics)
	if err != nil {
		return err
	}
	_, err = cl.makeRequestWithBody(routes.RegisterMetricsMethod, routes.RegisterMetricsPath, ContentTypeJSON, bytes.NewReader(toSend))
	if err != nil {
		return err
	}
	return err
}

func (cl *Client) IsMetricActive(string) bool {
	return false
}

func (cl *Client) BroadcastMessage(msg message.Message, nrHops int) {

}

func (cl *Client) RegisterBroadcastMessageHandler(msgId message.ID, handler func(message.Message)) {

}

func (cl *Client) QueryLocalMetric(metricName string, expression string) float64 {
	return 0
}

func (cl *Client) InstallLocalAggregationFunction(sourceMetricName, resultingMetricName, timeFrame, expression string) { // TODO queries

}

func (cl *Client) QueryPeerMetric(peer peer.Peer, metricName, expression string) float64 {
	return 0
}

func (cl *Client) SubscribeToPeerMetric(peer peer.Peer, destMetricName, peerMetricName, timeFrame string) {

}

func (cl *Client) InstallTreeAggregationFunction(nrHops int, sourceMetricName, resultingMetricName, timeFrame, expression string) { // TODO queries

}

func (e *Client) makeRequestWithBody(method, path, contentType string, body io.Reader) (*http.Response, error) {
	targetAddr := fmt.Sprintf("http://%s:%d:%s", e.conf.DemmonHostAddr, e.conf.DemmonPort, path)
	req, err := http.NewRequest(method, targetAddr, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 == 2 {
		return resp, nil
	} else {
		return nil, fmt.Errorf("non 2xx response, got: %s", resp.Status)
	}
}

func (e *Client) makeRequestNoBody(method, path string) (*http.Response, error) {
	targetAddr := fmt.Sprintf("http://%s:%d:%s", e.conf.DemmonHostAddr, e.conf.DemmonPort, path)
	req, err := http.NewRequest(method, targetAddr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 == 2 {
		return resp, nil
	} else {
		return nil, fmt.Errorf("non 2xx response, got: %s", resp.Status)
	}
}

func (e *Client) marshalMetricAsTextLine(metric timeseries.Metric) string {
	return fmt.Sprintf("%s/%s %s %d\n", metric.Service(), metric.Name(), base64.StdEncoding.EncodeToString([]byte(metric.Get().Marshal())), time.Now().UnixNano())
}

// func (f *Frontend) GetMetrics() []string {
// 	f.babel.SendRequest(monitoring.GetCurrMetricsReq(), f.proto.ID(), membership.ProtoID)
// 	ans := <-f.currRequest
// 	return ans.([]string)
// }

module github.com/nm-morais/demmon-client

go 1.15

require (
	github.com/gorilla/websocket v1.4.2
	github.com/mitchellh/mapstructure v1.3.3
	github.com/nm-morais/demmon-common v1.0.0
)

replace github.com/nm-morais/demmon-common => ../demmon-common

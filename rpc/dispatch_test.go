// Copyright 2012, 2013 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package rpc_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/juju/errors"
	"github.com/juju/loggo/v2"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/juju/internal/testing"
	"github.com/juju/juju/rpc"
	"github.com/juju/juju/rpc/jsoncodec"
)

type dispatchSuite struct {
	testing.BaseSuite
	server     *httptest.Server
	serverAddr string
	dead       chan error
	unique     int64
}

var _ = gc.Suite(&dispatchSuite{})

func (s *dispatchSuite) SetUpTest(c *gc.C) {
	s.BaseSuite.SetUpTest(c)

	loggo.GetLogger("juju.rpc").SetLogLevel(loggo.TRACE)
	s.dead = make(chan error, 1)

	unique := atomic.AddInt64(&s.unique, 1)
	mux := http.NewServeMux()

	mux.Handle(fmt.Sprintf("/rpc%d", unique), http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, err := websocketUpgrader.Upgrade(w, req, nil)
		if err != nil {
			c.Fatalf("failed to upgrade websocket: %v", err)
			return
		}
		defer c.Close()

		codec := jsoncodec.NewWebsocket(c)
		conn := rpc.NewConn(codec, nil)
		conn.Serve(&DispatchRoot{}, nil, nil)
		conn.Start(context.Background())

		select {
		case <-conn.Dead():
		case <-time.After(testing.LongWait):
			c.Fatalf("timeout waiting for connection to close")
		}

		select {
		case s.dead <- conn.Close():
		case <-time.After(testing.LongWait):
			c.Fatalf("timeout waiting for connection cleanup")
		}
	}))

	s.server = httptest.NewServer(mux)
	s.serverAddr = s.server.Listener.Addr().String()

	s.AddCleanup(func(*gc.C) {
		s.server.Close()
	})
}

var websocketUpgrader = &websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (s *dispatchSuite) TestWSWithoutParamsV0(c *gc.C) {
	s.assertRequestV0(c, `{"RequestId":1,"Type": "DispatchDummy","Id": "without","Request":"DoSomething"}`)
}

func (s *dispatchSuite) TestWSWithParamsV0(c *gc.C) {
	s.assertRequestV0(c, `{"RequestId":2,"Type": "DispatchDummy","Id": "with","Request":"DoSomething", "Params": {}}`)
}

func (s *dispatchSuite) TestWSWithoutParamsV1(c *gc.C) {
	s.assertRequestV1(c, `{"request-id":1,"type": "DispatchDummy","id": "without","request":"DoSomething"}`, `{"request-id":1,"response":{}}`)
}

func (s *dispatchSuite) TestWSWithParamsV1(c *gc.C) {
	s.assertRequestV1(c, `{"request-id":2,"type": "DispatchDummy","id": "with","request":"DoSomething", "params": {}}`, `{"request-id":2,"response":{}}`)
}

func (s *dispatchSuite) TestWSWithParamsV1Tracing(c *gc.C) {
	s.assertRequestV1(c,
		`{"request-id":2,"type": "DispatchDummy","id": "with","request":"DoSomething", "params": {}, "trace-id": "foobar", "span-id": "baz", "trace-flags": 1}`,
		`{"request-id":2,"response":{},"trace-id":"foobar","span-id":"baz","trace-flags":1}`,
	)
}

func (s *dispatchSuite) assertRequestV0(c *gc.C, req string) {
	err := s.sendRequestV0(c, req)
	c.Assert(errors.Is(err, errors.NotSupported), jc.IsTrue)
}

func (s *dispatchSuite) assertRequestV1(c *gc.C, req, expected string) {
	resp := s.sendRequestV1(c, req)
	c.Assert(resp, gc.Equals, expected+"\n")
}

// sendRequestV0 sends a V0 request and waits for a response.
func (s *dispatchSuite) sendRequestV0(c *gc.C, req string) error {
	ws := s.openWebSocket(c, req)
	defer ws.Close()

	go func() {
		_, _, err := ws.ReadMessage()
		c.Check(err, gc.NotNil)
	}()

	select {
	case err := <-s.dead:
		return err
	case <-time.After(testing.LongWait):
		c.Fatalf("timeout waiting for response")
		return nil
	}
}

// sendRequestV1 sends a V1 request and waits for a response.
func (s *dispatchSuite) sendRequestV1(c *gc.C, req string) string {
	ws := s.openWebSocket(c, req)
	defer ws.Close()

	result := make(chan string, 1)

	go func() {
		_, resp, err := ws.ReadMessage()
		c.Check(err, jc.ErrorIsNil)
		result <- string(resp)
	}()

	var resp string
	select {
	case resp = <-result:

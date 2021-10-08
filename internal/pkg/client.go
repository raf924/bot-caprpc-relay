package pkg

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/relay/client"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"net"
	capnp "zombiezen.com/go/capnproto2"
	"zombiezen.com/go/capnproto2/rpc"
)

var _ client.RelayClient = (*capnpClient)(nil)

type capnpClient struct {
	config      capnpClientConfig
	connector   connector.Connector
	messageChan chan domain.ServerMessage
}

type streamReceiver struct {
	messageChan chan domain.ServerMessage
	mapper      func(ptr capnp.Ptr) domain.ServerMessage
}

func (s *streamReceiver) Receive(receive connector.Connector_Receiver_receive) error {
	ptr, err := receive.Params.MessagePtr()
	if err != nil {
		return err
	}
	s.messageChan <- s.mapper(ptr)
	return nil
}

func newCapnpClient(config capnpClientConfig) *capnpClient {
	return &capnpClient{config: config}
}

func NewCapnpClient(config interface{}) client.RelayClient {
	data, err := yaml.Marshal(config)
	if err != nil {
		panic(err)
	}
	var conf capnpClientConfig
	if err := yaml.Unmarshal(data, &conf); err != nil {
		panic(err)
	}
	return newCapnpClient(conf)
}

func (c *capnpClient) connect() error {
	endpoint := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
	var conn net.Conn
	var err error
	if c.config.TLS.Enabled {
		tlsConfig, err := LoadMTLSClientConfig(c.config.TLS.Name, c.config.TLS.Ca, c.config.TLS.Cert, c.config.TLS.Key)
		if err != nil {
			return err
		}
		conn, err = tls.Dial("tcp", endpoint, tlsConfig)
		if err != nil {
			return err
		}
	} else {
		conn, err = net.Dial("tcp", endpoint)
		if err != nil {
			return err
		}
	}
	rpcConn := rpc.NewConn(rpc.StreamTransport(conn))
	c.connector = connector.Connector{Client: rpcConn.Bootstrap(context.TODO())}
	return nil
}

func (c *capnpClient) register(registration *domain.RegistrationMessage) (*domain.User, error) {
	results, err := c.connector.Register(context.TODO(), func(params connector.Connector_register_Params) error {
		registrationDTO, err := params.NewRegistration()
		if err != nil {
			return err
		}
		err = connector.MapRegistrationToDTO(registration, &registrationDTO)
		if err != nil {
			return err
		}
		return nil
	}).Struct()
	if err != nil {
		return nil, err
	}
	confirmation, err := results.Confirmation()
	if err != nil {
		return nil, err
	}
	user, err := confirmation.BotUser()
	if err != nil {
		return nil, err
	}
	type receiverParams interface {
		SetReceiver(receiver connector.Connector_Receiver) error
	}
	streamParamSetter := func(receiver connector.Connector_Receiver_Server) func(params receiverParams) error {
		return func(params receiverParams) error {
			cb := connector.Connector_Receiver_ServerToClient(receiver)
			err := params.SetReceiver(cb)
			if err != nil {
				return err
			}
			return nil
		}
	}
	c.connector.CommandStream(context.TODO(), func(params connector.Connector_commandStream_Params) error {
		return streamParamSetter(&streamReceiver{
			messageChan: c.messageChan,
			mapper: func(ptr capnp.Ptr) domain.ServerMessage {
				return connector.MapDTOToCommandMessage(connector.CommandPacket{Struct: ptr.Struct()})
			},
		})(params)
	})
	c.connector.MessageStream(context.TODO(), func(params connector.Connector_messageStream_Params) error {
		return streamParamSetter(&streamReceiver{
			messageChan: c.messageChan,
			mapper: func(ptr capnp.Ptr) domain.ServerMessage {
				return connector.MapDTOToChatMessage(connector.IncomingMessagePacket{Struct: ptr.Struct()})
			},
		})(params)
	})
	c.connector.EventStream(context.TODO(), func(params connector.Connector_eventStream_Params) error {
		return streamParamSetter(&streamReceiver{
			messageChan: c.messageChan,
			mapper: func(ptr capnp.Ptr) domain.ServerMessage {
				return connector.MapDTOToUserEvent(connector.UserPacket{Struct: ptr.Struct()})
			},
		})(params)
	})
	return connector.MapDTOToUser(user), nil
}

func (c *capnpClient) Connect(registration *domain.RegistrationMessage) (*domain.User, error) {
	err := c.connect()
	if err != nil {
		return nil, err
	}
	return c.register(registration)
}

func (c *capnpClient) Send(packet *domain.ClientMessage) error {
	_, err := c.connector.Send(context.TODO(), func(params connector.Connector_send_Params) error {
		message, err := params.NewMessage()
		if err != nil {
			return err
		}
		err = connector.MapClientMessageToDTO(packet, &message)
		if err != nil {
			return err
		}
		return nil
	}).Struct()
	if err != nil {
		return err
	}
	return nil
}

func (c *capnpClient) Recv() (domain.ServerMessage, error) {
	message, ok := <-c.messageChan
	if !ok {
		return nil, fmt.Errorf("cannot fetch server messages")
	}
	return message, nil
}

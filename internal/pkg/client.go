package pkg

import (
	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot-caprpc-relay/pkg"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/relay/client"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"net"
)

var _ client.RelayClient = (*capnpClient)(nil)

type capnpClient struct {
	config      pkg.CapnpClientConfig
	connector   connector.Connector
	messageChan chan domain.ServerMessage
}

type streamReceiver struct {
	messageChan chan domain.ServerMessage
	mapper      func(ptr capnp.Ptr) domain.ServerMessage
}

func (s *streamReceiver) Receive(ctx context.Context, receive connector.Connector_Receiver_receive) error {
	receive.Ack()
	ptr, err := receive.Args().Message()
	if err != nil {
		return err
	}
	s.messageChan <- s.mapper(ptr)
	return nil
}

func newCapnpClient(config pkg.CapnpClientConfig) *capnpClient {
	return &capnpClient{config: config}
}

func NewCapnpClient(config interface{}) client.RelayClient {
	data, err := yaml.Marshal(config)
	if err != nil {
		panic(err)
	}
	var conf pkg.CapnpClientConfig
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
	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), nil)
	c.connector = connector.Connector{Client: rpcConn.Bootstrap(context.TODO())}
	return nil
}

func (c *capnpClient) register(registration *domain.RegistrationMessage) (*domain.ConfirmationMessage, error) {
	answer, release := c.connector.Register(context.TODO(), func(params connector.Connector_register_Params) error {
		registrationDTO, err := params.NewRegistration()
		if err != nil {
			return err
		}
		err = connector.MapRegistrationToDTO(registration, &registrationDTO)
		if err != nil {
			return err
		}
		return nil
	})
	mapConfirmation := func() (*domain.ConfirmationMessage, error) {
		results, err := answer.Struct()
		if err != nil {
			return nil, err
		}
		confirmation, err := results.Confirmation()
		if err != nil {
			return nil, err
		}
		return connector.MapDTOToConfirmationMessage(confirmation), nil
	}
	confirmation, err := mapConfirmation()
	release()
	return confirmation, err
}

func (c *capnpClient) Connect(registration *domain.RegistrationMessage) (*domain.ConfirmationMessage, error) {
	err := c.connect()
	if err != nil {
		return nil, err
	}
	confirmation, err := c.register(registration)
	if err != nil {
		return nil, fmt.Errorf("failed to register: %v", err)
	}
	c.createCommandStreams()
	return confirmation, nil
}

func (c *capnpClient) Send(packet *domain.ClientMessage) error {
	answer, release := c.connector.Send(context.TODO(), func(params connector.Connector_send_Params) error {
		message, err := params.NewMessage()
		if err != nil {
			return err
		}
		err = connector.MapClientMessageToDTO(packet, &message)
		if err != nil {
			return err
		}
		return nil
	})
	_, err := answer.Struct()
	release()
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

func (c *capnpClient) createCommandStreams() {
	type receiverParams interface {
		SetReceiver(receiver connector.Connector_Receiver) error
	}
	streamParamSetter := func(receiver connector.Connector_Receiver_Server) func(params receiverParams) error {
		cb := connector.Connector_Receiver_ServerToClient(receiver, nil)
		return func(params receiverParams) error {
			err := params.SetReceiver(cb)
			if err != nil {
				return err
			}
			return nil
		}
	}
	go func() {
		answerCommand, release := c.connector.CommandStream(context.TODO(), func(params connector.Connector_commandStream_Params) error {
			return streamParamSetter(&streamReceiver{
				messageChan: c.messageChan,
				mapper: func(ptr capnp.Ptr) domain.ServerMessage {
					return connector.MapDTOToCommandMessage(connector.CommandPacket{Struct: ptr.Struct()})
				},
			})(params)
		})
		defer release()
		_, err := answerCommand.Struct()
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		answerMessage, release := c.connector.MessageStream(context.TODO(), func(params connector.Connector_messageStream_Params) error {
			return streamParamSetter(&streamReceiver{
				messageChan: c.messageChan,
				mapper: func(ptr capnp.Ptr) domain.ServerMessage {
					message := connector.MapDTOToChatMessage(connector.IncomingMessagePacket{Struct: ptr.Struct()})
					return message
				},
			})(params)
		})
		defer release()
		_, err := answerMessage.Struct()
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		answerEvent, release := c.connector.EventStream(context.TODO(), func(params connector.Connector_eventStream_Params) error {
			return streamParamSetter(&streamReceiver{
				messageChan: c.messageChan,
				mapper: func(ptr capnp.Ptr) domain.ServerMessage {
					return connector.MapDTOToUserEvent(connector.UserPacket{Struct: ptr.Struct()})
				},
			})(params)
		})
		defer release()
		_, err := answerEvent.Struct()
		if err != nil {
			panic(err)
		}
	}()
}

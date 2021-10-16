package pkg

import (
	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot-caprpc-relay/pkg"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/relay/server"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"log"
	"net"
)

var _ server.RelayServer = (*capnpRelayServer)(nil)

type capnpRelayServer struct {
	config     pkg.CapnpServerConfig
	commands   domain.CommandList
	botUser    *domain.User
	conn       net.Conn
	err        error
	connector  *connectorImpl
	dispatcher connector.Dispatcher
	doneChan   chan struct{}
}

func (c *capnpRelayServer) Done() <-chan struct{} {
	return c.doneChan
}

func (c *capnpRelayServer) Err() error {
	return c.err
}

func newCapnpRelayServer(config pkg.CapnpServerConfig) *capnpRelayServer {
	return &capnpRelayServer{
		doneChan: make(chan struct{}, 1),
		commands: domain.NewCommandList(),
		config:   config,
		connector: &connectorImpl{
			outgoingMessageChan: make(chan *domain.ClientMessage),
		},
	}
}

func NewCapnpRelayServer(config interface{}) server.RelayServer {
	data, err := yaml.Marshal(config)
	if err != nil {
		panic(err)
	}
	var conf pkg.CapnpServerConfig
	if err := yaml.Unmarshal(data, &conf); err != nil {
		panic(err)
	}
	return newCapnpRelayServer(conf)
}

func (c *capnpRelayServer) start() error {
	var listener net.Listener
	var err error
	listener, err = net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: int(c.config.Port),
	})
	if err != nil {
		return err
	}
	if c.config.TLS.Enabled {
		tlsConfig, err := LoadTLSServerConfig(c.config.TLS.Ca, c.config.TLS.Cert, c.config.TLS.Key)
		if err != nil {
			log.Println(err)
			return nil
		}
		listener = tls.NewListener(listener, tlsConfig)
	}
	go func(listener net.Listener) {
		for true {
			conn, err := listener.Accept()
			if err != nil {
				c.err = err
				c.doneChan <- struct{}{}
				return
			}
			rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), &rpc.Options{BootstrapClient: connector.Connector_ServerToClient(c.connector, nil).Client})
			c.dispatcher = connector.Dispatcher{Client: rpcConn.Bootstrap(context.TODO())}
			go func() {
				<-rpcConn.Done()
				_ = rpcConn.Close()
			}()
		}
	}(listener)
	return nil
}

func (c *capnpRelayServer) Start(botUser *domain.User, onlineUsers domain.UserList, trigger string) error {
	c.connector.onRegistration = func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error {
		if c.err != nil {
			return c.err
		}
		commands, err := registration.Commands()
		if err != nil {
			return err
		}
		c.commands = connector.MapDTOToCommandList(commands)
		err = connector.CreateConfirmationPacket(botUser, trigger, onlineUsers.All(), confirmation)
		if err != nil {
			return err
		}
		return nil
	}
	err := c.start()
	if err != nil {
		c.err = fmt.Errorf("start error: %v", err)
		c.doneChan <- struct{}{}
		return c.err
	}
	return nil
}

func (c *capnpRelayServer) Commands() domain.CommandList {
	return c.commands
}

func (c *capnpRelayServer) Send(message domain.ServerMessage) error {
	if c.err != nil {
		return fmt.Errorf("cannot send: %v", c.err)
	}
	//log.Println("Server", "Send", "start")
	var releaseFunc capnp.ReleaseFunc
	var err error
	switch message := message.(type) {
	case *domain.ChatMessage:
		var dispatchMessage connector.Dispatcher_dispatchMessage_Results_Future
		dispatchMessage, releaseFunc = c.dispatcher.DispatchMessage(context.TODO(), func(params connector.Dispatcher_dispatchMessage_Params) error {
			newMessage, err := params.NewMessage()
			if err != nil {
				return err
			}
			err = connector.MapChatMessageToDTO(message, newMessage)
			if err != nil {
				return err
			}
			return nil
		})
		_, err = dispatchMessage.Struct()
	case *domain.CommandMessage:
		var command connector.Dispatcher_dispatchCommand_Results_Future
		command, releaseFunc = c.dispatcher.DispatchCommand(context.TODO(), func(params connector.Dispatcher_dispatchCommand_Params) error {
			command, err := params.NewCommand()
			if err != nil {
				return err
			}
			err = connector.MapCommandMessageToDTO(message, command)
			if err != nil {
				return err
			}
			return nil
		})
		_, err = command.Struct()
	case *domain.UserEvent:
		var event connector.Dispatcher_dispatchEvent_Results_Future
		event, releaseFunc = c.dispatcher.DispatchEvent(context.TODO(), func(params connector.Dispatcher_dispatchEvent_Params) error {
			event, err := params.NewEvent()
			if err != nil {
				return err
			}
			err = connector.MapUserEventToDTO(message, event)
			if err != nil {
				return err
			}
			return nil
		})
		_, err = event.Struct()
	default:
		return fmt.Errorf("unknown message type")
	}
	releaseFunc()
	//log.Println("Server", "Send", "end")
	return err
}

func (c *capnpRelayServer) Recv() (*domain.ClientMessage, error) {
	if c.err != nil {
		return nil, c.err
	}
	packet, err := c.connector.recv()
	if err != nil {
		return nil, err
	}
	////log.Println("capnpRelayServer", "Recv", packet.Private())
	return packet, nil
}

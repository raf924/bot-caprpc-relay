package pkg

import (
	"capnproto.org/go/capnp/v3/rpc"
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
	config    pkg.CapnpServerConfig
	commands  domain.CommandList
	botUser   *domain.User
	conn      net.Conn
	err       error
	connector *connectorImpl
}

func newCapnpRelayServer(config pkg.CapnpServerConfig) *capnpRelayServer {
	return &capnpRelayServer{
		commands: domain.NewCommandList(),
		config:   config,
		connector: &connectorImpl{
			outgoingMessageChan: make(chan *connector.OutgoingMessagePacket),
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
				return
			}
			go func() {
				<-rpc.NewConn(rpc.NewStreamTransport(conn), &rpc.Options{BootstrapClient: connector.Connector_ServerToClient(c.connector, nil).Client}).Done()
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
		err = connector.CreateConfirmationPacket(botUser, trigger, onlineUsers.All(), &confirmation)
		if err != nil {
			return err
		}
		return nil
	}
	err := c.start()
	if err != nil {
		return err
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
	switch message := message.(type) {
	case *domain.ChatMessage:
		c.connector.sendMessage(message)
	case *domain.CommandMessage:
		c.connector.sendCommand(message)
	case *domain.UserEvent:
		c.connector.sendEvent(message)
	default:
		return fmt.Errorf("unknown message type")
	}
	return nil
}

func (c *capnpRelayServer) Recv() (*domain.ClientMessage, error) {
	if c.err != nil {
		return nil, c.err
	}
	packet, err := c.connector.recv()
	if err != nil {
		return nil, err
	}
	return connector.MapDTOToClientMessage(packet), nil
}

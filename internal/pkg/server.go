package pkg

import (
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/relay/server"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"log"
	"net"
	"zombiezen.com/go/capnproto2/rpc"
)

var _ server.RelayServer = (*capnpRelayServer)(nil)

type capnpRelayServer struct {
	config    capnpServerConfig
	commands  domain.CommandList
	botUser   *domain.User
	conn      net.Conn
	err       error
	connector *connectorImpl
}

func newCapnpRelayServer(config capnpServerConfig) *capnpRelayServer {
	return &capnpRelayServer{
		config: config,
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
	var conf capnpServerConfig
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
		log.Println(err)
		return nil
	}
	if c.config.TLS.Enabled {
		tlsConfig, err := LoadTLSServerConfig(c.config.TLS.Ca, c.config.TLS.Cert, c.config.TLS.Key)
		if err != nil {
			log.Println(err)
			return nil
		}
		listener = tls.NewListener(listener, tlsConfig)
	}
	conn, err := listener.Accept()
	if err != nil {
		return nil
	}
	go func() {
		c.err = rpc.NewConn(rpc.StreamTransport(conn), rpc.MainInterface(connector.Connector_ServerToClient(c.connector).Client)).Wait()
	}()
	return nil
}

func (c *capnpRelayServer) Start(botUser *domain.User, onlineUsers domain.UserList, trigger string) error {
	err := c.start()
	if err != nil {
		return err
	}
	c.connector.onRegistration = func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error {
		if c.err != nil {
			return c.err
		}
		commands, err := registration.Commands()
		if err != nil {
			return err
		}
		c.commands = connector.MapDTOToCommandList(commands)
		err = connector.CreateConfirmationPacket(botUser, onlineUsers.All(), &confirmation)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (c *capnpRelayServer) Commands() domain.CommandList {
	return c.commands
}

func (c *capnpRelayServer) Send(message domain.ServerMessage) error {
	if c.err != nil {
		return c.err
	}
	switch message := message.(type) {
	case *domain.ChatMessage:
		return c.connector.sendMessage(message)
	case *domain.CommandMessage:
		return c.connector.sendCommand(message)
	case *domain.UserEvent:
		return c.connector.sendEvent(message)
	default:
		return fmt.Errorf("unknown message type")
	}
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

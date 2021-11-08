package pkg

import (
	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot-caprpc-relay/pkg"
	"github.com/raf924/bot/pkg/domain"
	botRpc "github.com/raf924/bot/pkg/rpc"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"net"
	"sync"
)

var _ botRpc.ConnectorRelay = (*capnpConnectorRelay)(nil)

type rpcDuplex struct {
	conn       net.Conn
	rpcConn    *rpc.Conn
	connector  connector.Connector_Server
	dispatcher connector.Dispatcher
}

type RegistrationCallback func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error

type capnpConnectorRelay struct {
	config               pkg.CapnpServerConfig
	botUser              *domain.User
	listener             net.Listener
	err                  error
	connections          sync.Map //net.Conn
	doneChan             chan struct{}
	onRegistration       func(chan<- interface{}) RegistrationCallback
	clientMessageChannel chan *domain.ClientMessage
	ctx                  context.Context
	cleanUpFunc          context.CancelFunc
}

type dispatcher struct {
	duplex   rpcDuplex
	commands domain.CommandList
	err      error
}

func (d *dispatcher) Dispatch(message domain.ServerMessage) error {
	if d.err != nil {
		return fmt.Errorf("cannot dispatch: %v", d.err)
	}
	var err error
	var dispatch dispatchFunc
	switch message.(type) {
	case *domain.ChatMessage:
		dispatch = dispatchChatMessage
	case *domain.CommandMessage:
		dispatch = dispatchCommand
	case *domain.UserEvent:
		dispatch = dispatchEvent
	default:
		return fmt.Errorf("unknown message type")
	}
	releaseFunc, err := dispatch(d.duplex, message)
	releaseFunc()
	return fmt.Errorf("cannot dispatch: %v", err)
}

func (d *dispatcher) Commands() domain.CommandList {
	return d.commands
}

func (d *dispatcher) Done() <-chan struct{} {
	return d.duplex.rpcConn.Done()
}

func (d *dispatcher) Err() error {
	return d.err
}

var _ botRpc.Dispatcher = (*dispatcher)(nil)

func (c *capnpConnectorRelay) Accept() (botRpc.Dispatcher, error) {
	if c.err != nil {
		_ = c.listener.Close()
		return nil, c.err
	}
	connChan := make(chan net.Conn, 1)
	errChan := make(chan error, 1)
	go func() {
		conn, err := c.listener.Accept()
		if err != nil {
			c.err = err
			errChan <- err
			return
		}
		connChan <- conn
	}()
	var conn net.Conn
	select {
	case <-c.doneChan:
		return nil, c.err
	case err := <-errChan:
		_ = c.listener.Close()
		c.cleanUpFunc()
		return nil, err
	case conn = <-connChan:
		c.connections.Store(conn.RemoteAddr().String(), conn)
	}
	registrationChan := make(chan interface{})
	duplex := rpcDuplex{
		conn:      conn,
		connector: NewConnector(c.onRegistration(registrationChan), c.clientMessageChannel),
	}
	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), &rpc.Options{BootstrapClient: connector.Connector_ServerToClient(duplex.connector, nil).Client})
	duplex.rpcConn = rpcConn
	duplex.dispatcher = connector.Dispatcher{Client: rpcConn.Bootstrap(context.TODO())}
	return &dispatcher{
		duplex:   duplex,
		commands: (<-registrationChan).(domain.CommandList),
	}, nil
}

func (c *capnpConnectorRelay) Done() <-chan struct{} {
	return c.doneChan
}

func (c *capnpConnectorRelay) Err() error {
	return c.err
}

func newCapnpRelayServer(config pkg.CapnpServerConfig) *capnpConnectorRelay {
	return &capnpConnectorRelay{
		doneChan: make(chan struct{}, 1),
		config:   config,
	}
}

func NewCapnpRelayServer(config interface{}) botRpc.ConnectorRelay {
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

func (c *capnpConnectorRelay) start(ctx context.Context) error {
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
			return err
		}
		listener = tls.NewListener(listener, tlsConfig)
	}
	go func() {
		<-ctx.Done()
		err := listener.Close()
		if err != nil {
			c.err = err
		}
		c.cleanUpFunc()
	}()
	c.listener = listener
	return nil
}

func (c *capnpConnectorRelay) Start(ctx context.Context, botUser *domain.User, onlineUsers domain.UserList, trigger string) error {
	c.ctx = ctx
	var connectionCtx context.Context
	connectionCtx, c.cleanUpFunc = context.WithCancel(ctx)
	go func() {
		<-connectionCtx.Done()
		c.connections.Range(func(key, value interface{}) bool {
			_ = value.(net.Conn).Close()
			c.connections.Delete(key)
			return true
		})
		c.doneChan <- struct{}{}
	}()
	c.onRegistration = func(registrationChan chan<- interface{}) RegistrationCallback {
		return func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error {
			if c.err != nil {
				return c.err
			}
			commands, err := registration.Commands()
			if err != nil {
				return err
			}
			commandList := connector.MapDTOToCommandList(commands)
			registrationChan <- commandList
			err = connector.CreateConfirmationPacket(botUser, trigger, onlineUsers.All(), confirmation)
			if err != nil {
				return err
			}
			return nil
		}
	}
	c.clientMessageChannel = make(chan *domain.ClientMessage)
	err := c.start(ctx)
	if err != nil {
		c.err = fmt.Errorf("start error: %v", err)
		c.doneChan <- struct{}{}
		return c.err
	}
	return nil
}

func (c *capnpConnectorRelay) Recv() (*domain.ClientMessage, error) {
	if c.err != nil {
		return nil, c.err
	}
	message, ok := <-c.clientMessageChannel
	if !ok {
		c.err = fmt.Errorf("can't transmit messages")
		return nil, c.err
	}
	return message, nil
}

type dispatchFunc func(duplex rpcDuplex, message domain.ServerMessage) (capnp.ReleaseFunc, error)

var _ dispatchFunc = dispatchCommand
var _ dispatchFunc = dispatchChatMessage
var _ dispatchFunc = dispatchEvent

func dispatchCommand(duplex rpcDuplex, message domain.ServerMessage) (capnp.ReleaseFunc, error) {
	command, releaseFunc := duplex.dispatcher.DispatchCommand(context.TODO(), func(params connector.Dispatcher_dispatchCommand_Params) error {
		command, err := params.NewCommand()
		if err != nil {
			return err
		}
		err = connector.MapCommandMessageToDTO(message.(*domain.CommandMessage), command)
		if err != nil {
			return err
		}
		return nil
	})
	_, err := command.Struct()
	return releaseFunc, err
}

func dispatchChatMessage(duplex rpcDuplex, message domain.ServerMessage) (capnp.ReleaseFunc, error) {
	chatMessage, releaseFunc := duplex.dispatcher.DispatchMessage(context.TODO(), func(params connector.Dispatcher_dispatchMessage_Params) error {
		newMessage, err := params.NewMessage()
		if err != nil {
			return err
		}
		err = connector.MapChatMessageToDTO(message.(*domain.ChatMessage), newMessage)
		if err != nil {
			return err
		}
		return nil
	})
	_, err := chatMessage.Struct()
	return releaseFunc, err
}

func dispatchEvent(duplex rpcDuplex, message domain.ServerMessage) (capnp.ReleaseFunc, error) {
	event, releaseFunc := duplex.dispatcher.DispatchEvent(context.TODO(), func(params connector.Dispatcher_dispatchEvent_Params) error {
		event, err := params.NewEvent()
		if err != nil {
			return err
		}
		err = connector.MapUserEventToDTO(message.(*domain.UserEvent), event)
		if err != nil {
			return err
		}
		return nil
	})
	_, err := event.Struct()
	return releaseFunc, err
}

func (c *capnpConnectorRelay) dispatch(duplex rpcDuplex, message domain.ServerMessage, dispatch dispatchFunc) error {
	releaseFunc, err := dispatch(duplex, message)
	releaseFunc()
	return err
}

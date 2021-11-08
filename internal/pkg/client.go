package pkg

import (
	"capnproto.org/go/capnp/v3/rpc"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot-caprpc-relay/pkg"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/queue"
	botRpc "github.com/raf924/bot/pkg/rpc"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"net"
	"sync"
)

var _ botRpc.DispatcherRelay = (*capnpDispatcherRelay)(nil)

type capnpDispatcherRelay struct {
	config          pkg.CapnpClientConfig
	connector       connector.Connector
	messageConsumer *queue.Consumer
	err             error
	doneChan        chan struct{}
	mu              *sync.Mutex
}

func (c *capnpDispatcherRelay) Done() <-chan struct{} {
	return c.doneChan
}

func (c *capnpDispatcherRelay) Err() error {
	return c.err
}

func newCapnpClient(config pkg.CapnpClientConfig) *capnpDispatcherRelay {
	return &capnpDispatcherRelay{config: config, doneChan: make(chan struct{}, 1), mu: &sync.Mutex{}}
}

func NewCapnpClient(config interface{}) botRpc.DispatcherRelay {
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

func (c *capnpDispatcherRelay) connect() error {
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
	messageQueue := queue.NewQueue()
	producer, err := messageQueue.NewProducer()
	if err != nil {
		return err
	}
	c.messageConsumer, err = messageQueue.NewConsumer()
	rpcConn := rpc.NewConn(
		rpc.NewStreamTransport(conn),
		&rpc.Options{
			BootstrapClient: connector.Dispatcher_ServerToClient(
				&dispatcherServer{messageProducer: producer},
				nil,
			).Client,
		},
	)
	c.connector = connector.Connector{Client: rpcConn.Bootstrap(context.TODO())}
	go func() {
		select {
		case <-rpcConn.Done():
		}
		c.err = rpcConn.Close()
		c.doneChan <- struct{}{}
	}()
	return nil
}

func (c *capnpDispatcherRelay) register(registration *domain.RegistrationMessage) (*domain.ConfirmationMessage, error) {
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
	mapConfirmation := func(answer connector.Connector_register_Results_Future) (*domain.ConfirmationMessage, error) {
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
	confirmation, err := mapConfirmation(answer)
	_ = release
	return confirmation, err
}

func (c *capnpDispatcherRelay) Connect(registration *domain.RegistrationMessage) (*domain.ConfirmationMessage, error) {
	err := c.connect()
	if err != nil {
		return nil, err
	}
	confirmation, err := c.register(registration)
	if err != nil {
		c.err = fmt.Errorf("failed to register: %v", err)
		c.doneChan <- struct{}{}
		return nil, c.err
	}
	return confirmation, nil
}

func (c *capnpDispatcherRelay) Send(packet *domain.ClientMessage) error {
	answer, release := c.connector.Send(context.TODO(), func(params connector.Connector_send_Params) error {
		message, err := params.NewMessage()
		if err != nil {
			return err
		}
		err = connector.MapClientMessageToDTO(packet, message)
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

func (c *capnpDispatcherRelay) Recv() (domain.ServerMessage, error) {
	message, err := c.messageConsumer.Consume()
	if err != nil {
		return nil, fmt.Errorf("cannot fetch server messages")
	}
	return message.(domain.ServerMessage), nil
}

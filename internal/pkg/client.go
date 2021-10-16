package pkg

import (
	"capnproto.org/go/capnp/v3/rpc"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/raf924/bot-caprpc-relay/pkg"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/queue"
	"github.com/raf924/bot/pkg/relay/client"
	"github.com/raf924/connector-api/pkg/connector"
	"gopkg.in/yaml.v2"
	"net"
	"sync"
)

var _ client.RelayClient = (*capnpClient)(nil)

type capnpClient struct {
	config          pkg.CapnpClientConfig
	connector       connector.Connector
	messageConsumer *queue.Consumer
	err             error
	doneChan        chan struct{}
	mu              *sync.Mutex
}

func (c *capnpClient) Done() <-chan struct{} {
	return c.doneChan
}

func (c *capnpClient) Err() error {
	return c.err
}

func newCapnpClient(config pkg.CapnpClientConfig) *capnpClient {
	return &capnpClient{config: config, doneChan: make(chan struct{}, 1), mu: &sync.Mutex{}}
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
	messageQueue := queue.NewQueue()
	producer, err := messageQueue.NewProducer()
	if err != nil {
		return err
	}
	c.messageConsumer, err = messageQueue.NewConsumer()
	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), &rpc.Options{BootstrapClient: connector.Dispatcher_ServerToClient(&dispatcher{messageProducer: producer}, nil).Client})
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

func (c *capnpClient) Connect(registration *domain.RegistrationMessage) (*domain.ConfirmationMessage, error) {
	//log.Println("capnpClient", "Connect", "start")
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
	//log.Println("capnpClient", "Connect", "end")
	return confirmation, nil
}

func (c *capnpClient) Send(packet *domain.ClientMessage) error {
	//log.Println("capnpClient", "Send", "start")
	answer, release := c.connector.Send(context.TODO(), func(params connector.Connector_send_Params) error {
		message, err := params.NewMessage()
		if err != nil {
			return err
		}
		err = connector.MapClientMessageToDTO(packet, message)
		if err != nil {
			return err
		}
		//log.Println("capnpClient", "Send", "rpc", message.Private())
		return nil
	})
	_, err := answer.Struct()
	release()
	if err != nil {
		return err
	}
	//log.Println("capnpClient", "Send", "end")
	return nil
}

func (c *capnpClient) Recv() (domain.ServerMessage, error) {
	//log.Println("capnpClient", "Recv", "start")
	message, err := c.messageConsumer.Consume()
	if err != nil {
		return nil, fmt.Errorf("cannot fetch server messages")
	}
	//log.Println("capnpClient", "Recv", "end")
	return message.(domain.ServerMessage), nil
}

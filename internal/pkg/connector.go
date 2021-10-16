package pkg

import (
	"context"
	"fmt"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/connector-api/pkg/connector"
)

type connectorImpl struct {
	onRegistration      func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error
	onSend              func(send connector.Connector_send) error
	messageChan         chan *domain.ChatMessage
	commandChan         chan *domain.CommandMessage
	eventChan           chan *domain.UserEvent
	outgoingMessageChan chan *domain.ClientMessage
	err                 error
}

func (c *connectorImpl) recv() (*domain.ClientMessage, error) {
	//log.Println("connectorImpl", "recv", "start")
	message, ok := <-c.outgoingMessageChan
	if !ok {
		c.err = fmt.Errorf("can't transmit messages")
		return nil, c.err
	}
	//log.Println("connectorImpl", "recv", "end")
	return message, nil
}

func (c *connectorImpl) Register(ctx context.Context, register connector.Connector_register) error {
	//log.Println("connectorImpl", "Register", "start")
	register.Ack()
	registration, err := register.Args().Registration()
	if err != nil {
		return err
	}
	results, err := register.AllocResults()
	if err != nil {
		return err
	}
	confirmation, err := results.NewConfirmation()
	if err != nil {
		return err
	}
	err = c.onRegistration(registration, confirmation)
	//log.Println("connectorImpl", "Register", "end")
	return err
}

func (c *connectorImpl) Send(ctx context.Context, send connector.Connector_send) error {
	//log.Println("connectorImpl", "Send", "start")
	send.Ack()
	message, err := send.Args().Message()
	if err != nil {
		return err
	}
	c.outgoingMessageChan <- connector.MapDTOToClientMessage(message)
	//log.Println("connectorImpl", "Send", "end")
	return nil
}

package pkg

import (
	"context"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/connector-api/pkg/connector"
)

type connectorImpl struct {
	onRegistration      RegistrationCallback
	onSend              func(send connector.Connector_send) error
	messageChan         chan *domain.ChatMessage
	commandChan         chan *domain.CommandMessage
	eventChan           chan *domain.UserEvent
	outgoingMessageChan chan<- *domain.ClientMessage
	err                 error
}

func NewConnector(
	onRegistration RegistrationCallback,
	outgoingMessageChan chan<- *domain.ClientMessage,
) connector.Connector_Server {
	return &connectorImpl{
		onRegistration:      onRegistration,
		outgoingMessageChan: outgoingMessageChan,
	}
}

func (c *connectorImpl) Register(ctx context.Context, register connector.Connector_register) error {
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
	return err
}

func (c *connectorImpl) Send(ctx context.Context, send connector.Connector_send) error {
	send.Ack()
	message, err := send.Args().Message()
	if err != nil {
		return err
	}
	c.outgoingMessageChan <- connector.MapDTOToClientMessage(message)
	return nil
}

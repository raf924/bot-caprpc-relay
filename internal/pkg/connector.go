package pkg

import (
	"context"
	"fmt"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/connector-api/pkg/connector"
	"log"
	capnp "zombiezen.com/go/capnproto2"
)

type connectorImpl struct {
	onRegistration      func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error
	onSend              func(send connector.Connector_send) error
	messageReceiver     connector.Connector_Receiver
	commandReceiver     connector.Connector_Receiver
	eventReceiver       connector.Connector_Receiver
	outgoingMessageChan chan *connector.OutgoingMessagePacket
	err                 error
}

func (c *connectorImpl) recv() (*connector.OutgoingMessagePacket, error) {
	message, closed := <-c.outgoingMessageChan
	if closed {
		c.err = fmt.Errorf("can't transmit messages")
		return nil, c.err
	}
	return message, nil
}

func (c *connectorImpl) Register(register connector.Connector_register) error {
	registration, err := register.Params.Registration()
	if err != nil {
		return err
	}
	confirmation, err := register.Results.NewConfirmation()
	if err != nil {
		return err
	}
	return c.onRegistration(registration, confirmation)
}

func (c *connectorImpl) Send(send connector.Connector_send) error {
	message, err := send.Params.Message()
	if err != nil {
		return err
	}
	c.outgoingMessageChan <- &message
	return nil
}

func (c *connectorImpl) sendMessage(message *domain.ChatMessage) error {
	_, err := c.messageReceiver.Receive(context.TODO(), func(params connector.Connector_Receiver_receive_Params) error {
		_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		if err != nil {
			return err
		}
		packet, err := connector.NewRootIncomingMessagePacket(seg)
		if err != nil {
			return err
		}
		err = connector.MapChatMessageToDTO(message, &packet)
		if err != nil {
			return err
		}
		err = params.SetMessage(packet)
		if err != nil {
			return err
		}
		log.Println("Sending message", message.Message())
		return nil
	}).Struct()
	if err != nil {
		return fmt.Errorf("cannot send message: %v", err)
	}
	return nil
}

func (c *connectorImpl) sendCommand(command *domain.CommandMessage) error {
	c.commandReceiver.Receive(context.TODO(), func(params connector.Connector_Receiver_receive_Params) error {
		_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		if err != nil {
			return err
		}
		packet, err := connector.NewRootCommandPacket(seg)
		if err != nil {
			return err
		}
		err = connector.MapCommandMessageToDTO(command, &packet)
		if err != nil {
			return err
		}
		err = params.SetMessage(packet)
		if err != nil {
			return err
		}
		return nil
	})
	return nil
}

func (c *connectorImpl) sendEvent(event *domain.UserEvent) error {
	c.eventReceiver.Receive(context.TODO(), func(params connector.Connector_Receiver_receive_Params) error {
		_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		if err != nil {
			return err
		}
		packet, err := connector.NewRootUserPacket(seg)
		if err != nil {
			return err
		}
		err = connector.MapUserEventToDTO(event, &packet)
		if err != nil {
			return err
		}
		err = params.SetMessage(packet)
		if err != nil {
			return err
		}
		return nil
	})
	return nil
}

func (c *connectorImpl) MessageStream(stream connector.Connector_messageStream) error {
	if !stream.Params.HasReceiver() {
		return fmt.Errorf("no receiver")
	}
	c.messageReceiver = stream.Params.Receiver()
	return nil
}

func (c *connectorImpl) CommandStream(stream connector.Connector_commandStream) error {
	if !stream.Params.HasReceiver() {
		return fmt.Errorf("no receiver")
	}
	c.commandReceiver = stream.Params.Receiver()
	return nil
}

func (c *connectorImpl) EventStream(stream connector.Connector_eventStream) error {
	if !stream.Params.HasReceiver() {
		return fmt.Errorf("no receiver")
	}
	c.eventReceiver = stream.Params.Receiver()
	return nil
}

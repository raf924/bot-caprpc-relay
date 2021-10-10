package pkg

import (
	"capnproto.org/go/capnp/v3"
	"context"
	"fmt"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/connector-api/pkg/connector"
)

type connectorImpl struct {
	onRegistration      func(registration connector.RegistrationPacket, confirmation connector.ConfirmationPacket) error
	onSend              func(send connector.Connector_send) error
	messageReceiver     connector.Connector_Receiver
	commandReceiver     connector.Connector_Receiver
	eventReceiver       connector.Connector_Receiver
	messageChan         chan *domain.ChatMessage
	commandChan         chan *domain.CommandMessage
	eventChan           chan *domain.UserEvent
	outgoingMessageChan chan *connector.OutgoingMessagePacket
	err                 error
}

func (c *connectorImpl) recv() (*connector.OutgoingMessagePacket, error) {
	message, ok := <-c.outgoingMessageChan
	if !ok {
		c.err = fmt.Errorf("can't transmit messages")
		return nil, c.err
	}
	return message, nil
}

func (c *connectorImpl) Register(ctx context.Context, register connector.Connector_register) error {
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
	register.Ack()
	return err
}

func (c *connectorImpl) Send(ctx context.Context, send connector.Connector_send) error {
	send.Ack()
	message, err := send.Args().Message()
	if err != nil {
		return err
	}
	c.outgoingMessageChan <- &message
	return nil
}

func (c *connectorImpl) sendMessage(message *domain.ChatMessage) {
	c.messageChan <- message
}

func (c *connectorImpl) sendCommand(command *domain.CommandMessage) {
	c.commandChan <- command
}

func (c *connectorImpl) sendEvent(event *domain.UserEvent) {
	c.eventChan <- event
}

func (c *connectorImpl) sendToReceiver(ctx context.Context, message domain.ServerMessage, receiver connector.Connector_Receiver, mapper func(message domain.ServerMessage, segment *capnp.Segment) error) error {
	receive, releaseFunc := receiver.Receive(ctx, func(params connector.Connector_Receiver_receive_Params) error {
		_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		if err != nil {
			return err
		}
		err = mapper(message, seg)
		if err != nil {
			return err
		}
		root, err := seg.Message().Root()
		if err != nil {
			return err
		}
		err = params.SetMessage(root)
		if err != nil {
			return err
		}
		return nil
	})
	_, err := receive.Struct()
	releaseFunc()
	return err
}

func (c *connectorImpl) MessageStream(ctx context.Context, stream connector.Connector_messageStream) error {
	stream.Ack()
	if !stream.Args().HasReceiver() {
		return fmt.Errorf("no receiver")
	}
	c.messageChan = make(chan *domain.ChatMessage)
	mapper := func(message domain.ServerMessage, segment *capnp.Segment) error {
		packet, err := connector.NewRootIncomingMessagePacket(segment)
		if err != nil {
			return err
		}
		err = connector.MapChatMessageToDTO(message.(*domain.ChatMessage), &packet)
		if err != nil {
			return err
		}
		return nil
	}
	messageReceiver := stream.Args().Receiver()
	for c.err == nil {
		var message domain.ServerMessage
		var ok bool
		select {
		case message, ok = <-c.messageChan:
			break
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			c.err = fmt.Errorf("cannot transmit packets")
			return c.err
		}
		err := c.sendToReceiver(ctx, message, messageReceiver, mapper)
		if err != nil {
			return err
		}
		stream.Ack()
	}
	return c.err
}

func (c *connectorImpl) CommandStream(ctx context.Context, stream connector.Connector_commandStream) error {
	stream.Ack()
	if !stream.Args().HasReceiver() {
		return fmt.Errorf("no receiver")
	}
	c.commandChan = make(chan *domain.CommandMessage)
	commandReceiver := stream.Args().Receiver()
	for c.err == nil {
		var command domain.ServerMessage
		var ok bool
		select {
		case command, ok = <-c.commandChan:
			break
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			c.err = fmt.Errorf("cannot transmit packets")
			return c.err
		}
		commandReceiver.Receive(ctx, func(params connector.Connector_Receiver_receive_Params) error {
			_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
			if err != nil {
				return err
			}
			packet, err := connector.NewRootCommandPacket(seg)
			if err != nil {
				return err
			}
			err = connector.MapCommandMessageToDTO(command.(*domain.CommandMessage), &packet)
			if err != nil {
				return err
			}
			err = params.SetMessage(packet.ToPtr())
			if err != nil {
				return err
			}
			return nil
		})
	}
	return nil
}

func (c *connectorImpl) EventStream(ctx context.Context, stream connector.Connector_eventStream) error {
	stream.Ack()
	if !stream.Args().HasReceiver() {
		return fmt.Errorf("no receiver")
	}
	c.eventChan = make(chan *domain.UserEvent)
	eventReceiver := stream.Args().Receiver()
	for c.err == nil {
		var event *domain.UserEvent
		var ok bool
		select {
		case event, ok = <-c.eventChan:
			break
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			c.err = fmt.Errorf("cannot transmit packets")
			return c.err
		}
		eventReceiver.Receive(context.TODO(), func(params connector.Connector_Receiver_receive_Params) error {
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
			err = params.SetMessage(packet.ToPtr())
			if err != nil {
				return err
			}
			return nil
		})
	}
	return c.err
}

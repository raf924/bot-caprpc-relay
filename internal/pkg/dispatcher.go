package pkg

import (
	"context"
	"fmt"
	"github.com/raf924/bot/pkg/domain"
	"github.com/raf924/bot/pkg/queue"
	"github.com/raf924/connector-api/pkg/connector"
)

var _ connector.Dispatcher_Server = (*dispatcherServer)(nil)

type dispatcherServer struct {
	messageProducer *queue.Producer
}

func (d *dispatcherServer) dispatch(ctx context.Context, packet interface{}, mapper func(packet interface{}) domain.ServerMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := d.messageProducer.Produce(mapper(packet))
		if err != nil {
			return fmt.Errorf("dispatch error: %v", err)
		}
	}
	return nil
}

func (d *dispatcherServer) DispatchMessage(ctx context.Context, message connector.Dispatcher_dispatchMessage) error {
	message.Ack()
	packet, err := message.Args().Message()
	if err != nil {
		return fmt.Errorf("failed to dispatch message: %v", err)
	}
	return d.dispatch(ctx, packet, func(packet interface{}) domain.ServerMessage {
		return connector.MapDTOToChatMessage(packet.(connector.IncomingMessagePacket))
	})
}

func (d *dispatcherServer) DispatchCommand(ctx context.Context, command connector.Dispatcher_dispatchCommand) error {
	command.Ack()
	packet, err := command.Args().Command()
	if err != nil {
		return fmt.Errorf("failed to dispatch command: %v", err)
	}
	return d.dispatch(ctx, packet, func(packet interface{}) domain.ServerMessage {
		return connector.MapDTOToCommandMessage(packet.(connector.CommandPacket))
	})
}

func (d *dispatcherServer) DispatchEvent(ctx context.Context, event connector.Dispatcher_dispatchEvent) error {
	event.Ack()
	packet, err := event.Args().Event()
	if err != nil {
		return fmt.Errorf("failed to dispatch event: %v", err)
	}
	return d.dispatch(ctx, packet, func(packet interface{}) domain.ServerMessage {
		return connector.MapDTOToUserEvent(packet.(connector.UserPacket))
	})
}

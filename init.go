package bot_caprpc_relay

import (
	"github.com/raf924/bot-caprpc-relay/internal/pkg"
	"github.com/raf924/bot/pkg/rpc"
)

func init() {
	rpc.RegisterDispatcherRelay("capnp", pkg.NewCapnpClient)
	rpc.RegisterConnectorRelay("capnp", pkg.NewCapnpRelayServer)
}

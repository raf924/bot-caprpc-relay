package bot_caprpc_relay

import (
	"github.com/raf924/bot-caprpc-relay/v2/internal/pkg"
	"github.com/raf924/connector-sdk/rpc"
)

func init() {
	rpc.RegisterDispatcherRelay("capnp", pkg.NewCapnpClient)
	rpc.RegisterConnectorRelay("capnp", pkg.NewCapnpRelayServer)
}

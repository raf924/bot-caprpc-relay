package bot_caprpc_relay

import (
	"github.com/raf924/bot-caprpc-relay/internal/pkg"
	"github.com/raf924/bot/pkg/relay/client"
	"github.com/raf924/bot/pkg/relay/server"
)

func init() {
	client.RegisterRelayClient("capnp", pkg.NewCapnpClient)
	server.RegisterRelayServer("capnp", pkg.NewCapnpRelayServer)
}

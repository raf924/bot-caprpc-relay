package pkg

type CapnpServerConfig struct {
	Port    int32  `yaml:"port"`
	Timeout string `yaml:"timeout"`
	TLS     struct {
		Enabled bool     `yaml:"enabled"`
		Ca      string   `yaml:"ca"`
		Cert    string   `yaml:"cert"`
		Key     string   `yaml:"key"`
		Users   []string `yaml:"users"`
	} `yaml:"tls"`
}

type CapnpClientConfig struct {
	Host string `yaml:"host"`
	Port int32  `yaml:"port"`
	TLS  struct {
		Enabled bool   `yaml:"enabled"`
		Name    string `yaml:"name"`
		Ca      string `yaml:"ca"`
		Cert    string `yaml:"cert"`
		Key     string `yaml:"key"`
	}
}

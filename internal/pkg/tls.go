package pkg

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"path"
)

func loadCaPool(caFile string) (*x509.CertPool, error) {
	caPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(path.Clean(caFile))
	if err != nil {
		return nil, err
	}
	if !caPool.AppendCertsFromPEM(pem) {
		return nil, errors.New("couldn't use CA certificate")
	}
	return caPool, nil
}

func loadTlsConfig(caFile string, certFile string, keyFile string) (*tls.Config, *x509.CertPool, error) {
	certificate, err := tls.LoadX509KeyPair(path.Clean(certFile), path.Clean(keyFile))
	if err != nil {
		return nil, nil, err
	}
	caPool, err := loadCaPool(caFile)
	if err != nil {
		return nil, nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}, caPool, nil
}

func LoadTLSServerConfig(caFile string, certFile string, keyFile string) (*tls.Config, error) {
	tlsConfig, caPool, err := loadTlsConfig(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.ClientCAs = caPool
	tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	return tlsConfig, nil
}

func LoadMTLSClientConfig(serverName string, caFile string, certFile string, keyFile string) (*tls.Config, error) {
	tlsConfig, caPool, err := loadTlsConfig(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.RootCAs = caPool
	tlsConfig.ServerName = serverName
	return tlsConfig, nil
}

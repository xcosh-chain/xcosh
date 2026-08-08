package internal

import (
	"fmt"
	"net"
	"xcosh/internal/p2p"
)

// SyncEngine membungkus p2p.P2PServer untuk menangani sinkronisasi node post-quantum.
type SyncEngine struct {
	server *p2p.P2PServer
	port   string
}

// NewSyncEngine menginisialisasi instance SyncEngine baru dengan standar Dilithium3.
func NewSyncEngine(listenPort string) (*SyncEngine, error) {
	// Generate atau gunakan key pair Dilithium default untuk node identity
	var privKey p2p.DilithiumPrivateKey
	// Inisialisasi dummy/default key untuk bootstrap
	for i := range privKey {
		privKey[i] = byte(i % 256)
	}

	config := p2p.P2PConfig{
		PrivateKey:      &privKey,
		MaxPeers:        50,
		NoDiscovery:     false,
		DiscoveryV4:     true,
		ListenAddr:      listenPort,
		Name:            "Xcosh-Node/1.0.0",
		EnableMsgEvents: true,
	}

	srv := p2p.NewServer(config)
	if srv == nil {
		return nil, fmt.Errorf("gagal membuat server P2P")
	}

	return &SyncEngine{
		server: srv,
		port:   listenPort,
	}, nil
}

// Start menjalankan engine sinkronisasi P2P
func (s *SyncEngine) Start() error {
	return s.server.Start()
}

// Stop menghentikan engine sinkronisasi P2P
func (s *SyncEngine) Stop() {
	s.server.Stop()
}

// ConnectToPeer melakukan koneksi keluar ke peer target
func (s *SyncEngine) ConnectToPeer(peerAddr string) error {
	addr, err := net.ResolveTCPAddr("tcp", peerAddr)
	if err != nil {
		return err
	}
	
	node := p2p.NewNodeV4(nil, addr.IP, addr.Port, addr.Port)
	s.server.AddPeer(node)
	return nil
}

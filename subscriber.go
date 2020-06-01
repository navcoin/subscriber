package subscriber

import (
	zmq "github.com/pebbe/zmq4"
	log "github.com/sirupsen/logrus"
)

type Subscriber struct {
	socket  *zmq.Socket
	address string
}

func NewSubscriber(address string) *Subscriber {
	return &Subscriber{address: address}
}

func (s *Subscriber) Subscribe(callback func()) error {
	log.Info("Subscribe to ZMQ")

	var err error
	s.socket, err = zmq.NewSocket(zmq.SUB)
	if err != nil {
		log.WithError(err).Error("Failed to open new 0MQ socket")
		return err
	}
	defer s.close()

	if err := s.socket.Connect(s.address); err != nil {
		log.WithError(err).Errorf("Failed to connect to %s", s.address)
		return err
	}

	if err := s.socket.SetSubscribe("hashblock"); err != nil {
		log.WithError(err).Error("Failed to subscribe to ZMQ")
		return err
	}

	log.Info("Waiting for ZMQ messages")
	for {
		msg, err := s.socket.Recv(0)
		if err != nil {
			log.WithError(err).Error("Failed to receive message")
			return err
		}

		if msg == "hashblock" {
			log.Info("New Block found")
			callback()
		}
	}
}

func (s *Subscriber) close() {
	err := s.socket.Close()
	if err != nil {
		log.WithError(err).Fatal("Failed to close socket")
	}
}

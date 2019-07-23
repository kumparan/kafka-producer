package kafka

type (
	// KafkaMessage :nodoc:
	KafkaMessage struct {
		ID     int64  `json:"id"`
		UserID int64  `json:"user_id"`
		Body   string `json:"body,omitempty"`
		Time   string `json:"time"`
	}

	kafkaMessageWithSubject struct {
		Topic   string `json:"topic"`
		Message []byte `json:"message"`
	}
)

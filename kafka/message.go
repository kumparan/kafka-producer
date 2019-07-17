package kafka

type (
	kafkaMessageWithSubject struct {
		Topic   string `json:"topic"`
		Message []byte `json:"message"`
	}
)

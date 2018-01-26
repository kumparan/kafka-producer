package main

import (
	"net/http"

	"github.com/kumparan/go-lib/router"
)

func registerServiceEndpoints(r *router.Router) {
	kafkaGroup := r.SubRouter("/api/v1")
	kafkaGroup.Post("/publish", func(w http.ResponseWriter, r *http.Request) {

	})
}

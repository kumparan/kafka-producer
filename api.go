package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/kumparan/go-lib/http/httpresponse"
	"github.com/kumparan/go-lib/router"
	"github.com/kumparan/kafka-producer/kafka"
)

func registerServiceEndpoints(r *router.Router) {
	kafkaGroup := r.SubRouter("/api/v1")
	kafkaGroup.Post("/publish", func(w http.ResponseWriter, r *http.Request) {
		jsonContent, err := ioutil.ReadAll(r.Body)
		if err != nil {
			httpresponse.InternalServerError(w, err.Error())
			return
		}
		message := kafka.Message{}
		err = json.Unmarshal(jsonContent, &message)
		if err != nil {
			httpresponse.BadRequest(w, err.Error())
			return
		}
		err = kafka.Publish(message)
		if err != nil {
			httpresponse.InternalServerError(w, err.Error())
			return
		}else{
			httpresponse.StatusOk(w)
			return
		}
	})
}

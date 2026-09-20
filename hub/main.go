package main

import (
	"fmt"
	"net/http"
	"net/url"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleSubscription)

	fmt.Println("Hub listening on :8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}

func handleSubscription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid Form Data", http.StatusBadRequest)
		return
	}

	mode := r.PostForm.Get("hub.mode")
	if mode != "subscribe" {
		http.Error(w, "Invalid Hub Mode", http.StatusBadRequest)
		return
	}

	topic := r.PostForm.Get("hub.topic")
	callback := r.PostForm.Get("hub.callback")
	secret := r.PostForm.Get("hub.secret")

	if topic == "" || callback == "" || secret == "" {
		http.Error(w, "Missing Subscription Data", http.StatusBadRequest)
		return
	}

	callbackURL, err := url.Parse(callback)
	if err != nil || (callbackURL.Scheme != "http" && callbackURL.Scheme != "https") || callbackURL.Host == "" {
		http.Error(w, "Invalid Callback URL", http.StatusBadRequest)
		return
	}

	fmt.Printf(
		"Subscription request topic=%q callback=%q\n",
		topic,
		callback,
	)

	w.WriteHeader(http.StatusAccepted)
}

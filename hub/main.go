package main

import (
	"fmt"
	"net/http"
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

	fmt.Printf(
		"request mode=%q topic=%q callback=%q has_secret=%t\n",
		r.PostForm.Get("hub.mode"),
		r.PostForm.Get("hub.topic"),
		r.PostForm.Get("hub.callback"),
		r.PostForm.Get("hub.secret") != "",
	)

	w.WriteHeader(http.StatusAccepted)
}

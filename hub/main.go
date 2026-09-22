package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const leaseSeconds = 86400

type subscriptionKey struct {
	Topic    string
	Callback string
}

type subscription struct {
	Topic     string
	Callback  string
	Secret    string
	ExpiresAt time.Time
}

type hub struct {
	client        *http.Client
	subscriptions map[subscriptionKey]subscription
	mu            sync.Mutex
}

func main() {
	h := &hub{
		client: &http.Client{
			Timeout: 5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		subscriptions: make(map[subscriptionKey]subscription),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{$}", h.handleSubscription)
	mux.HandleFunc("POST /publish", h.handlePublish)

	fmt.Println("Hub listening on :8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}

func (h *hub) handleSubscription(w http.ResponseWriter, r *http.Request) {
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
	if err != nil || callbackURL.Scheme != "http" || callbackURL.Host == "" {
		http.Error(w, "Invalid Callback URL", http.StatusBadRequest)
		return
	}

	sub := subscription{
		Topic:    topic,
		Callback: callback,
		Secret:   secret,
	}

	fmt.Printf(
		"Subscription request topic=%q callback=%q\n",
		sub.Topic,
		sub.Callback,
	)

	w.WriteHeader(http.StatusAccepted)

	go h.verifySubscription(sub)
}

func (h *hub) handlePublish(w http.ResponseWriter, r *http.Request) {
	payload, err := json.Marshal(map[string]string{
		"message": "hello from hub",
	})
	if err != nil {
		http.Error(w, "Error generating payload", http.StatusInternalServerError)
		return
	}

	now := time.Now()

	h.mu.Lock()
	subs := make([]subscription, 0, len(h.subscriptions))
	for key, sub := range h.subscriptions {
		if !now.Before(sub.ExpiresAt) {
			delete(h.subscriptions, key)
			continue
		}
		subs = append(subs, sub)
	}
	h.mu.Unlock()

	delivered := 0

	for _, sub := range subs {
		if err := h.deliver(sub, payload); err != nil {
			fmt.Printf("delivery failed callback=%q error=%v\n", sub.Callback, err)
			continue
		}
		delivered++
	}

	fmt.Fprintf(w, "Published to %d subscribers\n", delivered)
}

func (h *hub) deliver(sub subscription, payload []byte) error {
	mac := hmac.New(sha256.New, []byte(sub.Secret))
	mac.Write(payload)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequest(http.MethodPost, sub.Callback, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature", signature)

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("subscriber returned status %d", resp.StatusCode)
	}

	return nil
}

func (h *hub) verifySubscription(sub subscription) {
	challenge, err := newChallenge()
	if err != nil {
		fmt.Printf("Error generating challenge: %v\n", err)
		return
	}

	callbackURL, err := url.Parse(sub.Callback)
	if err != nil {
		fmt.Printf("Error parsing callback URL: %v\n", err)
		return
	}

	query := callbackURL.Query()
	query.Set("hub.mode", "subscribe")
	query.Set("hub.topic", sub.Topic)
	query.Set("hub.challenge", challenge)
	query.Set("hub.lease_seconds", strconv.Itoa(leaseSeconds))
	callbackURL.RawQuery = query.Encode()

	resp, err := h.client.Get(callbackURL.String())
	if err != nil {
		fmt.Printf("Error verifying subscription: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Printf("Subscription verification failed with status: %d\n", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading verification response: %v\n", err)
		return
	}

	if string(body) != challenge {
		fmt.Println("Subscription verification failed: challenge mismatch")
		return
	}

	sub.ExpiresAt = time.Now().Add(time.Duration(leaseSeconds) * time.Second)

	key := subscriptionKey{
		Topic:    sub.Topic,
		Callback: sub.Callback,
	}

	h.mu.Lock()
	h.subscriptions[key] = sub
	active := len(h.subscriptions)
	h.mu.Unlock()

	fmt.Printf(
		"Subscription verified topic=%q callback=%q active=%d\n",
		sub.Topic,
		sub.Callback,
		active,
	)
}

func newChallenge() (string, error) {
	bytes := make([]byte, 16)

	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

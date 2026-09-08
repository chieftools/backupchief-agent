package pusher

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	websocketHandshakeTimeout = 10 * time.Second
	websocketWriteTimeout     = 10 * time.Second
	websocketCloseTimeout     = 2 * time.Second
	defaultReconnectDelay     = 2 * time.Second
	maximumReconnectDelay     = 5 * time.Minute
	defaultActivityTimeout    = 60
	maximumMessageBytes       = 1 << 20
)

// Config describes a Realtime WebSocket connection.
type Config struct {
	Key       string `json:"key"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Channel   string `json:"channel"`
	AuthURL   string `json:"auth_url"`
	Encrypted bool   `json:"encrypted,omitempty"`
}

// Message is a Pusher protocol message.
type Message struct {
	Event   string          `json:"event"`
	Data    json.RawMessage `json:"data"`
	Channel string          `json:"channel,omitempty"`
}

// UnmarshalData decodes Pusher's nested JSON data into target.
func (message Message) UnmarshalData(target any) error {
	var data string
	if err := json.Unmarshal(message.Data, &data); err != nil {
		return fmt.Errorf("failed to unmarshal data string: %w", err)
	}
	if err := json.Unmarshal([]byte(data), target); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}
	return nil
}

// MessageHandler handles a Realtime event.
type MessageHandler func(message Message)

// ConnectionEstablishedMessageData contains the Pusher connection details.
type ConnectionEstablishedMessageData struct {
	SocketID        string `json:"socket_id"`
	ActivityTimeout int    `json:"activity_timeout"`
	SocketChiefColo string `json:"socketchief_colo,omitempty"`
}

// Client manages a Realtime WebSocket connection.
type Client struct {
	config         *Config
	credential     string
	userAgent      string
	protocol       string
	verbose        bool
	httpClient     *http.Client
	conn           *websocket.Conn
	socketID       string
	isConnected    bool
	isConnecting   bool
	connectCancel  context.CancelFunc
	generation     uint64
	reconnectTimer *time.Timer
	reconnectDelay time.Duration
	stopChan       chan struct{}
	eventHandlers  map[string]MessageHandler
	mu             sync.RWMutex
	writeMu        sync.Mutex
	pingTicker     *time.Ticker
	pingStop       chan struct{}
}

// NewClient creates a Realtime client.
func NewClient(config *Config, userAgent, credential, protocol string, httpClient *http.Client, verbose bool) *Client {
	if config == nil || credential == "" {
		return nil
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: websocketWriteTimeout}
	}

	return &Client{
		config:         config,
		credential:     credential,
		userAgent:      userAgent,
		protocol:       protocol,
		verbose:        verbose,
		httpClient:     httpClient,
		stopChan:       make(chan struct{}),
		eventHandlers:  make(map[string]MessageHandler),
		reconnectDelay: defaultReconnectDelay,
	}
}

// Start connects and keeps retrying until the client is disconnected.
func (c *Client) Start() {
	if err := c.Connect(); err != nil {
		log.Printf("[realtime] Initial connection failed: %v", err)
		c.scheduleReconnect()
	}
}

// Connect establishes a Realtime connection.
func (c *Client) Connect() error {
	c.mu.Lock()

	if c.isStoppedLocked() {
		c.mu.Unlock()
		return fmt.Errorf("realtime client is disconnected")
	}

	if c.config == nil {
		c.mu.Unlock()
		return fmt.Errorf("realtime configuration is missing")
	}

	if c.conn != nil || c.isConnecting {
		c.mu.Unlock()
		return nil
	}

	config := *c.config
	userAgent := c.userAgent
	generation := c.generation
	ctx, cancel := context.WithTimeout(context.Background(), websocketHandshakeTimeout)
	c.isConnecting = true
	c.connectCancel = cancel
	c.mu.Unlock()
	defer cancel()

	websocketURL := buildWebSocketURLFromConfig(&config)
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = websocketHandshakeTimeout

	connection, _, err := dialer.DialContext(ctx, websocketURL, map[string][]string{
		"User-Agent": {userAgent},
	})

	c.mu.Lock()
	activeAttempt := c.generation == generation
	if activeAttempt {
		c.isConnecting = false
		c.connectCancel = nil
	}
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	if !activeAttempt || c.isStoppedLocked() || c.config == nil || !sameConfig(c.config, &config) || c.conn != nil {
		c.mu.Unlock()
		c.closeWebSocket(connection, websocket.CloseNormalClosure, "connection superseded")
		return nil
	}

	c.conn = connection
	connection.SetReadLimit(maximumMessageBytes)
	c.socketID = ""
	c.isConnected = false
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}
	c.reconnectDelay = defaultReconnectDelay
	c.mu.Unlock()

	log.Printf("[realtime] Connected to %s", config.Host)

	go c.handleMessages(connection)

	return nil
}

// IsConnected reports whether the client has an active connection.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isConnected
}

// UpdateConfig replaces the Realtime configuration and reconnects when needed.
func (c *Client) UpdateConfig(newConfig *Config) error {
	c.mu.Lock()

	if newConfig == nil {
		c.mu.Unlock()
		c.disconnect("configuration removed")
		return nil
	}

	if c.isStoppedLocked() {
		c.mu.Unlock()
		return fmt.Errorf("realtime client is disconnected")
	}

	configChanged := c.config == nil ||
		c.config.Key != newConfig.Key ||
		c.config.Host != newConfig.Host ||
		c.config.Port != newConfig.Port ||
		c.config.Channel != newConfig.Channel ||
		c.config.AuthURL != newConfig.AuthURL ||
		c.config.Encrypted != newConfig.Encrypted

	c.config = newConfig

	// If config changed, replace any current connection or in-flight connection attempt.
	var connToClose *websocket.Conn
	var cancelConnect context.CancelFunc
	shouldReconnect := false

	if configChanged {
		log.Printf("[realtime] Configuration changed, reconnecting...")

		c.generation++
		cancelConnect = c.connectCancel
		c.connectCancel = nil
		connToClose = c.conn
		c.conn = nil
		c.socketID = ""
		c.isConnected = false
		c.isConnecting = false
		c.stopPingTickerLocked()

		if c.reconnectTimer != nil {
			c.reconnectTimer.Stop()
			c.reconnectTimer = nil
		}

		shouldReconnect = true
	}

	c.mu.Unlock()

	if cancelConnect != nil {
		cancelConnect()
	}
	if connToClose != nil {
		c.closeWebSocket(connToClose, websocket.CloseNormalClosure, "configuration changed")
	}

	if shouldReconnect {
		go func() {
			if err := c.Connect(); err != nil {
				log.Printf("[realtime] Failed to reconnect with new config: %v", err)
				c.scheduleReconnect()
			}
		}()
	}

	return nil
}

// OnEvent registers a handler for an event type.
func (c *Client) OnEvent(eventType string, handler MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventHandlers[eventType] = handler
}

// Disconnect closes the Realtime connection.
func (c *Client) Disconnect() {
	c.disconnect("client disconnecting")
}

func (c *Client) disconnect(reason string) {
	c.mu.Lock()

	select {
	case <-c.stopChan:
	default:
		close(c.stopChan)
	}

	c.generation++
	cancelConnect := c.connectCancel
	c.connectCancel = nil
	connToClose := c.conn
	c.conn = nil
	c.socketID = ""

	c.stopPingTickerLocked()

	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}

	c.isConnected = false
	c.isConnecting = false
	c.config = nil
	c.mu.Unlock()

	if cancelConnect != nil {
		cancelConnect()
	}
	if connToClose != nil {
		c.closeWebSocket(connToClose, websocket.CloseNormalClosure, reason)
	}

	log.Printf("[realtime] Disconnected")
}

func buildWebSocketURLFromConfig(config *Config) string {
	if config == nil {
		return ""
	}

	scheme := "ws"
	if config.Encrypted {
		scheme = "wss"
	}

	return fmt.Sprintf(
		"%s://%s:%d/app/%s?protocol=7&client=backupchief&version=1.0.0",
		scheme,
		config.Host,
		config.Port,
		config.Key,
	)
}

func (c *Client) handleMessages(conn *websocket.Conn) {
	defer func() {
		shouldReconnect := false
		wasActive := false

		c.mu.Lock()
		if c.conn == conn {
			wasActive = true
			c.conn = nil
			c.socketID = ""
			c.isConnected = false
			c.stopPingTickerLocked()
			shouldReconnect = !c.isStoppedLocked() && c.config != nil
		}
		c.mu.Unlock()

		if wasActive {
			conn.Close()
		}

		if shouldReconnect {
			if c.verbose {
				log.Printf("[realtime] Connection lost, scheduling reconnect...")
			}

			c.scheduleReconnect()
		}
	}()

	for {
		select {
		case <-c.stopChan:
			return
		default:
		}

		var message Message
		err := conn.ReadJSON(&message)

		if err != nil {
			if c.isConnectionActive(conn) {
				log.Printf("[realtime] Failed to read message: %v", err)
			}
			return
		}

		c.handlePusherMessage(conn, message)
	}
}

func (c *Client) handlePusherMessage(conn *websocket.Conn, message Message) {
	if !c.isConnectionActive(conn) {
		return
	}

	if c.verbose {
		log.Printf("[realtime] %s => %s", message.Event, message.Data)
	}

	switch message.Event {
	case "pusher:connection_established":
		var data ConnectionEstablishedMessageData
		if err := message.UnmarshalData(&data); err == nil {
			c.mu.Lock()
			if c.conn != conn || c.isStoppedLocked() {
				c.mu.Unlock()
				return
			}
			c.socketID = data.SocketID
			c.isConnected = true
			c.mu.Unlock()

			if c.verbose {
				log.Printf("[realtime] Connection established, socket ID: %s @ colo: %s", data.SocketID, data.SocketChiefColo)
			}

			c.startPingTicker(conn, data.ActivityTimeout)

			go func() {
				if err := c.subscribeToChannel(conn, data.SocketID); err != nil {
					log.Printf("[realtime] Failed to subscribe to channel: %v", err)
				}
			}()
		} else {
			log.Printf("[realtime] Failed to parse connection established data: %v", err)
		}

	case "pusher:ping":
		if err := c.sendMessageOnConn(conn, Message{Event: "pusher:pong"}); err != nil {
			log.Printf("[realtime] Failed to send pong message: %v", err)
		}

	case "pusher:pong":
		return

	case "pusher:error":
		log.Printf("[realtime] Received error: %s", string(message.Data))

	case "pusher_internal:subscription_succeeded":
		if c.verbose {
			log.Printf("[realtime] Successfully subscribed to channel: %s", message.Channel)
		}
		c.notifyEventListeners(message)

	default:
		if !c.notifyEventListeners(message) {
			log.Printf("[realtime] Received unhandled event: %s", message.Event)
		}
	}
}

func (c *Client) startPingTicker(conn *websocket.Conn, activityTimeout int) {
	if activityTimeout <= 0 {
		activityTimeout = defaultActivityTimeout
	}

	ticker := time.NewTicker(time.Duration(activityTimeout) * time.Second)
	stop := make(chan struct{})

	c.mu.Lock()
	if c.conn != conn || c.isStoppedLocked() {
		c.mu.Unlock()
		ticker.Stop()
		return
	}
	c.stopPingTickerLocked()
	c.pingTicker = ticker
	c.pingStop = stop
	c.mu.Unlock()

	go func() {
		for {
			select {
			case <-ticker.C:
				if err := c.sendMessageOnConn(conn, Message{Event: "pusher:ping"}); err != nil && c.verbose {
					log.Printf("[realtime] Failed to send ping message: %v", err)
				}
			case <-stop:
				return
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *Client) sendMessageOnConn(conn *websocket.Conn, message Message) error {
	if conn == nil {
		return fmt.Errorf("not connected")
	}

	if message.Data == nil {
		message.Data = json.RawMessage(`{}`)
	}

	encoded, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	if !c.isConnectionActive(conn) {
		return fmt.Errorf("connection no longer active")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if !c.isConnectionActive(conn) {
		return fmt.Errorf("connection no longer active")
	}

	if err := conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, encoded); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (c *Client) subscribeToChannel(conn *websocket.Conn, socketID string) error {
	c.mu.RLock()
	if c.conn != conn || c.config == nil || c.isStoppedLocked() {
		c.mu.RUnlock()
		return fmt.Errorf("not connected")
	}
	config := *c.config
	credential := c.credential
	userAgent := c.userAgent
	protocol := c.protocol
	verbose := c.verbose
	c.mu.RUnlock()

	if config.Channel == "" {
		return fmt.Errorf("no channel configured")
	}

	if conn == nil {
		return fmt.Errorf("not connected")
	}

	var subscribeData map[string]any

	if strings.HasPrefix(config.Channel, "private-") {
		auth, err := c.generateChannelAuth(socketID, config.Channel, config.AuthURL, credential, userAgent, protocol)
		if err != nil {
			c.closeWebSocket(conn, websocket.ClosePolicyViolation, "channel authorization failed")
			return fmt.Errorf("failed to generate auth: %w", err)
		}

		subscribeData = map[string]any{
			"channel": config.Channel,
			"auth":    auth,
		}
	} else {
		subscribeData = map[string]any{
			"channel": config.Channel,
		}
	}

	data, _ := json.Marshal(subscribeData)
	message := Message{
		Event: "pusher:subscribe",
		Data:  data,
	}

	if err := c.sendMessageOnConn(conn, message); err != nil {
		return fmt.Errorf("failed to send subscribe message: %w", err)
	}

	if verbose {
		log.Printf("[realtime] Subscribing to channel: %s", config.Channel)
	}

	return nil
}

func (c *Client) generateChannelAuth(socketID, channel, authURL, credential, userAgent, protocol string) (string, error) {
	if authURL == "" {
		return "", fmt.Errorf("auth URL required for private channels")
	}

	form := url.Values{}
	form.Set("socket_id", socketID)
	form.Set("channel_name", channel)

	request, err := http.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create auth request: %w", err)
	}

	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("User-Agent", userAgent)
	request.Header.Set("BackupChief-Protocol-Revision", protocol)

	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("auth request failed: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth request returned status %d", response.StatusCode)
	}

	var authResponse struct {
		Auth string `json:"auth"`
	}
	if err := json.NewDecoder(response.Body).Decode(&authResponse); err != nil {
		return "", fmt.Errorf("failed to decode auth response: %w", err)
	}

	return authResponse.Auth, nil
}

func (c *Client) notifyEventListeners(message Message) bool {
	handled := false

	c.mu.RLock()
	for eventType, handler := range c.eventHandlers {
		if eventType == message.Event {
			go handler(message)

			handled = true
		}
	}
	c.mu.RUnlock()

	return handled
}

func (c *Client) scheduleReconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isStoppedLocked() || c.config == nil || c.conn != nil || c.isConnecting {
		return
	}

	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
	}

	delay := fullJitter(c.reconnectDelay)
	c.reconnectDelay *= 2
	if c.reconnectDelay > maximumReconnectDelay {
		c.reconnectDelay = maximumReconnectDelay
	}
	generation := c.generation

	c.reconnectTimer = time.AfterFunc(delay, func() {
		c.mu.RLock()
		stopped := c.isStoppedLocked() || c.generation != generation
		c.mu.RUnlock()
		if stopped {
			return
		}

		if c.verbose {
			log.Printf("[realtime] Attempting to reconnect...")
		}

		if err := c.Connect(); err != nil {
			log.Printf("[realtime] Reconnection failed: %v", err)
			c.scheduleReconnect()
		}
	})
}

func fullJitter(maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(maximum)+1))
	if err != nil {
		return maximum
	}
	return time.Duration(value.Int64())
}

func (c *Client) closeWebSocket(conn *websocket.Conn, code int, reason string) {
	if conn == nil {
		return
	}

	deadline := time.Now().Add(websocketCloseTimeout)
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
	_ = conn.Close()
}

func (c *Client) stopPingTickerLocked() {
	if c.pingTicker != nil {
		c.pingTicker.Stop()
		c.pingTicker = nil
	}
	if c.pingStop != nil {
		close(c.pingStop)
		c.pingStop = nil
	}
}

func (c *Client) isStoppedLocked() bool {
	select {
	case <-c.stopChan:
		return true
	default:
		return false
	}
}

func (c *Client) isConnectionActive(conn *websocket.Conn) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn == conn && !c.isStoppedLocked()
}

func sameConfig(a, b *Config) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.Key == b.Key &&
		a.Host == b.Host &&
		a.Port == b.Port &&
		a.Channel == b.Channel &&
		a.AuthURL == b.AuthURL &&
		a.Encrypted == b.Encrypted
}

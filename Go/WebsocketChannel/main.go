package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrder = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("Client conected (total: %d)", len(h.clients))

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("cliente desconectado (total: %d)", len(h.clients))
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

func (c *Client) readPump(h *Hub) {
	defer func() {
		h.unregister <- c
		c.conn.Close()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("reading error: %v", err)
			}
			break
		}
		h.broadcast <- message
	}
}

func (c *Client) writePump() {
	defer c.conn.Close()

	for message := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
			log.Printf("error while writing: %v", err)
			return
		}
	}
	c.conn.WriteMessage(websocket.CloseMessage, []byte{})
}

func serverWs(h *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrder.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error while upgrading: ", err)
		return
	}
	client := &Client{conn: conn, send: make(chan []byte, 256)}
	h.register <- client

	go client.writePump()
	go client.readPump(h)
}

func main() {
	hub := newHub()
	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serverWs(hub, w, r)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(homePage))
	})

	log.Println("Server listenning http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Error starting the server: ", err)
	}
}

const homePage = `
<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><title>Chat WebSocket</title></head>
<body>
  <h2>Chat de prueba</h2>
  <div id="mensajes" style="border:1px solid #ccc; height:200px; overflow-y:auto; padding:8px;"></div>
  <input id="input" type="text" placeholder="Escribe un mensaje..." style="width:80%;">
  <button onclick="enviar()">Enviar</button>
 
  <script>
    const ws = new WebSocket("ws://" + window.location.host + "/ws");
    const mensajes = document.getElementById("mensajes");
 
    ws.onmessage = (event) => {
      const p = document.createElement("p");
      p.textContent = event.data;
      mensajes.appendChild(p);
      mensajes.scrollTop = mensajes.scrollHeight;
    };
 
    function enviar() {
      const input = document.getElementById("input");
      if (input.value.trim() === "") return;
      ws.send(input.value);
      input.value = "";
    }
 
    document.getElementById("input").addEventListener("keyup", (e) => {
      if (e.key === "Enter") enviar();
    });
  </script>
</body>
</html>
`

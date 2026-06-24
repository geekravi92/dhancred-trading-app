const io = require("socket.io")(3000, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 20000,
});

console.log("Socket.IO test server started on port 3000");
console.log("Server version: 2.5.0");
console.log("Ping interval: 25000ms");
console.log("Ping timeout: 20000ms");

// Handle connections to root namespace
io.on("connection", (socket) => {
  console.log(`[${new Date().toISOString()}] Client connected: ${socket.id}`);

  // Log ping/pong events at Engine.IO level (like in socket.io-client)
  // Events should be on socket.conn, not on socket
  socket.conn.on("ping", () => {
    console.log(
      `[${new Date().toISOString()}] [${
        socket.id
      }] 🔵 [ping event] Sending PING to client`
    );
  });

  socket.conn.on("pong", () => {
    console.log(
      `[${new Date().toISOString()}] [${
        socket.id
      }] ✅ [pong event] Received PONG from client`
    );
  });

  // Also log all packets for debugging (at Engine.IO level)
  socket.conn.on("packet", (packet) => {
    if (packet.type !== "ping" && packet.type !== "pong") {
      console.log(
        `[${new Date().toISOString()}] [${socket.id}] [packet] Received: type=${
          packet.type
        }, data=${JSON.stringify(packet.data)}`
      );
    }
  });

  // Also log all events at Engine.IO level
  socket.conn.on("upgrade", () => {
    console.log(
      `[${new Date().toISOString()}] [${socket.id}] Connection upgraded`
    );
  });

  socket.conn.on("close", (reason) => {
    console.log(
      `[${new Date().toISOString()}] [${
        socket.id
      }] Connection closed: ${reason}`
    );
  });

  // Log data sending at transport level (if available)
  try {
    console.log(
      `[${new Date().toISOString()}] [${
        socket.id
      }] Setting up transport.write hook...`
    );
    console.log(
      `[${new Date().toISOString()}] [${socket.id}] transport:`,
      socket.conn.transport ? "exists" : "null"
    );
    console.log(
      `[${new Date().toISOString()}] [${socket.id}] transport.socket:`,
      socket.conn.transport && socket.conn.transport.socket ? "exists" : "null"
    );

    if (socket.conn.transport && socket.conn.transport.socket) {
      const originalWrite = socket.conn.transport.socket.write;
      if (originalWrite) {
        console.log(
          `[${new Date().toISOString()}] [${
            socket.id
          }] ✅ Hooked transport.write`
        );
        socket.conn.transport.socket.write = function (data) {
          if (typeof data === "string") {
            // Check if this is PING (text message "2")
            if (data === "2" || data.startsWith("2")) {
              console.log(
                `[${new Date().toISOString()}] [${
                  socket.id
                }] 🔵 [transport.write] Sending PING: "${data}"`
              );
            } else if (data === "3" || data.startsWith("3")) {
              console.log(
                `[${new Date().toISOString()}] [${
                  socket.id
                }] 🟢 [transport.write] Sending PONG: "${data}"`
              );
            } else if (data.length < 100) {
              console.log(
                `[${new Date().toISOString()}] [${
                  socket.id
                }] [transport.write] Sending: "${data}"`
              );
            }
          } else {
            console.log(
              `[${new Date().toISOString()}] [${
                socket.id
              }] [transport.write] Sending non-string data:`,
              typeof data,
              data instanceof Buffer ? `Buffer(${data.length})` : data
            );
          }
          return originalWrite.apply(this, arguments);
        };
      } else {
        console.log(
          `[${new Date().toISOString()}] [${
            socket.id
          }] ⚠️ transport.socket.write is not a function`
        );
      }
    } else {
      console.log(
        `[${new Date().toISOString()}] [${
          socket.id
        }] ⚠️ transport or transport.socket not available`
      );
    }
  } catch (e) {
    console.log(
      `[${new Date().toISOString()}] [${
        socket.id
      }] ❌ Could not hook transport.write: ${e.message}`
    );
    console.error(e);
  }

  // Handler for 'test-server' event
  socket.on("test-server", () => {
    console.log(`[${socket.id}] Received 'test-server' event`);
    socket.emit("test-client", { foo: "bar" });
  });

  // Disconnect handler
  socket.on("disconnect", (reason) => {
    console.log(
      `[${new Date().toISOString()}] Client disconnected: ${
        socket.id
      }, reason: ${reason}`
    );
  });
});

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("\nShutting down server...");
  io.close(() => {
    console.log("Server closed");
    process.exit(0);
  });
});

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket connection required.", {
          status: 426
        });
      }

      const id = env.CHAT_ROOM.idFromName("hora-tv-global-chat");
      const room = env.CHAT_ROOM.get(id);

      return room.fetch(request);
    }

    if (url.pathname === "/") {
      return new Response(
        "HORA TV Chat Worker is running.",
        {
          headers: {
            "Content-Type": "text/plain"
          }
        }
      );
    }

    return new Response("Not found.", {
      status: 404
    });
  }
};


export class ChatRoom extends DurableObject {

  constructor(ctx, env) {
    super(ctx, env);

    this.ctx = ctx;
    this.env = env;

    this.sessions = new Map();
    this.lastMessageTime = new Map();
  }


  async fetch(request) {

    if (
      request.headers.get("Upgrade") !==
      "websocket"
    ) {
      return new Response(
        "WebSocket connection required.",
        {
          status: 426
        }
      );
    }

    const pair = new WebSocketPair();

    const client = pair[0];
    const server = pair[1];

    server.accept();

    const sessionId =
      crypto.randomUUID();

    this.sessions.set(
      sessionId,
      server
    );

    server.send(
      JSON.stringify({
        type: "welcome",
        message: "Connected to HORA TV Live Chat.",
        online: this.sessions.size
      })
    );

    server.send(
      JSON.stringify({
        type: "history",
        messages: await this.getMessages()
      })
    );

    this.broadcast(
      {
        type: "online",
        online: this.sessions.size
      }
    );

    server.addEventListener(
      "message",
      async event => {

        try {

          const data =
            JSON.parse(event.data);

          await this.handleMessage(
            sessionId,
            server,
            data
          );

        }
        catch (error) {

          server.send(
            JSON.stringify({
              type: "error",
              message:
                "Invalid message."
            })
          );

        }

      }
    );


    server.addEventListener(
      "close",
      () => {

        this.sessions.delete(
          sessionId
        );

        this.broadcast(
          {
            type: "online",
            online: this.sessions.size
          }
        );

      }
    );


    server.addEventListener(
      "error",
      () => {

        this.sessions.delete(
          sessionId
        );

      }
    );


    return new Response(
      null,
      {
        status: 101,
        webSocket: client
      }
    );
  }


  async handleMessage(
    sessionId,
    socket,
    data
  ) {

    if (
      data.type !== "message"
    ) {
      return;
    }


    const username =
      this.cleanText(
        data.username,
        20
      );

    const message =
      this.cleanText(
        data.message,
        300
      );


    if (!username) {

      socket.send(
        JSON.stringify({
          type: "error",
          message:
            "Please enter a username."
        })
      );

      return;
    }


    if (!message) {

      socket.send(
        JSON.stringify({
          type: "error",
          message:
            "Message cannot be empty."
        })
      );

      return;
    }


    const now =
      Date.now();

    const last =
      this.lastMessageTime.get(
        sessionId
      ) || 0;


    if (
      now - last <
      1200
    ) {

      socket.send(
        JSON.stringify({
          type: "error",
          message:
            "Please wait a moment before sending another message."
        })
      );

      return;
    }


    this.lastMessageTime.set(
      sessionId,
      now
    );


    let replyTo = null;


    if (
      data.replyTo &&
      typeof data.replyTo === "object"
    ) {

      replyTo = {

        id:
          this.cleanText(
            data.replyTo.id,
            50
          ),

        username:
          this.cleanText(
            data.replyTo.username,
            20
          ),

        message:
          this.cleanText(
            data.replyTo.message,
            120
          )

      };

    }


    const chatMessage = {

      id:
        crypto.randomUUID(),

      username,

      message,

      replyTo,

      time: now

    };


    await this.saveMessage(
      chatMessage
    );


    this.broadcast(
      {
        type: "message",
        message: chatMessage
      }
    );

  }


  cleanText(
    value,
    maxLength
  ) {

    if (
      typeof value !== "string"
    ) {
      return "";
    }


    return value
      .trim()
      .replace(
        /[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g,
        ""
      )
      .slice(
        0,
        maxLength
      );

  }


  async getMessages() {

    const data =
      await this.ctx.storage.get(
        "messages"
      );

    if (
      !Array.isArray(data)
    ) {
      return [];
    }

    return data;

  }


  async saveMessage(
    message
  ) {

    let messages =
      await this.getMessages();


    messages.push(
      message
    );


    if (
      messages.length >
      100
    ) {

      messages =
        messages.slice(
          -100
        );

    }


    await this.ctx.storage.put(
      "messages",
      messages
    );

  }


  broadcast(
    data
  ) {

    const payload =
      JSON.stringify(
        data
      );


    for (
      const [
        sessionId,
        socket
      ]
      of this.sessions
    ) {

      try {

        socket.send(
          payload
        );

      }
      catch (error) {

        this.sessions.delete(
          sessionId
        );

      }

    }

  }

}

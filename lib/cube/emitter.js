var util = require("util"),
    websocket = require("websocket");

module.exports = function() {
  var emitter = {},
      queue = [],
      url,
      socket,
      timeout,
      closing;

  function close() {
    if (socket) {
      util.log("closing socket");
      socket.removeListener("error", reopen);
      socket.removeListener("close", reopen);
      socket.close();
      socket = null;
    }
  }

  function closeWhenDone() {
    closing = true;
    if (socket) {
      if (!socket.bytesWaitingToFlush) close();
      else setTimeout(closeWhenDone, 1000);
    }
  }

  function open() {
    timeout = 0;
    close();
    util.log("opening socket: " + url);
    var client = new websocket.client();
    client.on("connect", function(connection) {
      socket = connection;
      socket.on("message", log);
      socket.on("error", reopen);
      socket.on("close", reopen);
      flush();
    });
    client.on("connectFailed", reopen);
    client.on("error", reopen);
    client.connect(url);
  }

  function reopen() {
    if (!timeout && !closing) {
      util.log("reopening soon");
      timeout = setTimeout(open, 1000);
    }
  }

  function flush() {
    var event;
    while (event = queue.pop()) {
      try {
        socket.sendUTF(JSON.stringify(event));
      } catch (e) {
        util.log(e.stack);
        reopen();
        return queue.push(event);
      }
    }
  }

  function log(message) {
    util.log(message.utf8Data);
  }

  emitter.open = function(host, port) {
    url = "ws://" + host + ":" + port + "/1.0/event/put";
    open();
    return emitter;
  };

  emitter.send = function(event) {
    queue.push(event);
    if (socket) flush();
    return emitter;
  };

  emitter.close = function() {
    closeWhenDone();
    return emitter;
  };

  return emitter;
};

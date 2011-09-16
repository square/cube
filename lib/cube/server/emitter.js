var util = require("util"),
    websocket = require("websocket");

module.exports = function() {
  var emitter = {},
      queue = [],
      url,
      socket,
      timeout;

  function close() {
    if (socket) {
      util.log("closing socket");
      socket.close();
      socket = null;
    }
  }

  function open() {
    timeout = 0;
    close();
    util.log("opening socket: " + url);
    var client = new websocket.client();
    client.on("connect", function(connection) { socket = connection; flush(); });
    client.on("connectFailed", reopen);
    client.connect(url);
  }

  function reopen() {
    if (!timeout) {
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
    close();
    return emitter;
  };

  return emitter;
};

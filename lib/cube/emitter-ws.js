'use strict';

var util      = require("util"),
    websocket = require("websocket"),
    metalog   = require("./metalog");

module.exports = function(protocol, host, port) {
  var emitter = {},
      queue = [],
      url = protocol + "\/\/" + host + ":" + port + "/1.0/event/put",
      socket,
      timeout,
      closing;

  function close() {
    metalog.warn('cube_emitter', {is: 'closing socket', emitter: emitter.report()});
    if (socket) {
      socket.removeListener("error", reopen);
      socket.removeListener("close", reopen);
      socket.close();
      socket = null;
    }
  }

  function closeWhenDone() {
    closing = true;
    if (socket) {
      metalog.warn('cube_emitter', {is: 'closing when done', emitter: emitter.report()});
      if (!socket.bytesWaitingToFlush) close();
      else setTimeout(closeWhenDone, 1000);
    }
  }

  function open() {
    timeout = 0;
    close();
    metalog.warn('cube_emitter', {is: 'opening socket', url: url});
    var client = new websocket.client();
    client.on("connect", function(connection) {
      socket = connection;
      socket.on("message", log);
      socket.on("error", reopen);
      socket.on("close", reopen);
      flush();
      if (closing) closeWhenDone();
    });
    client.on("connectFailed", reopen);
    client.on("error", reopen);
    client.connect(url);
  }

  function reopen() {
    if (!timeout && !closing) {
      metalog.warn('cube_emitter', {is: 'reopening soon', delay: 1000});
      timeout = setTimeout(open, 1000);
    }
  }

  function flush() {
    var event;
    while (event = queue.pop()) {
      try {
        socket.sendUTF(JSON.stringify(event));
      } catch (err) {
        metalog.warn('cube_emitter', {is: 'error', error: err.message, stack: er.stack});
        reopen();
        return queue.push(event);
      }
    }
  }

  function log(message) {
    metalog.minor('cube_emitter', {is: 'response', message: message.utf8Data, emitter: emitter.report()});
  }

  emitter.report = function report(){
    return {pending: (socket && socket.bytesWaitingToFlush), socket: (socket ? 'open' : 'closed'), url: url};
  }

  emitter.send = function(event) {
    metalog.trace('emitter_send', event);
    queue.push(event);
    if (socket) flush();
    return emitter;
  };

  emitter.close = function() {
    closeWhenDone();
    return emitter;
  };

  open();

  return emitter;
};

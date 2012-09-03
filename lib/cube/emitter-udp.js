'use strict';

var util = require("util"),
    dgram = require("dgram");

module.exports = function(protocol, host, port) {
  var emitter = {},
      queue = [],
      udp = dgram.createSocket("udp4"),
      closing;

  if (protocol != "udp:") throw new Error("invalid UDP protocol");

  function send() {
    var event = queue.pop();
    if (!event) return;
    var buffer = new Buffer(JSON.stringify(event));
    udp.send(buffer, 0, buffer.length, port, host, function(error) {
      if (error) console.warn(error);
      if (queue.length) process.nextTick(send);
      else if (closing) udp.close();
    });
  }

  emitter.send = function(event) {
    if (!closing && queue.push(event) == 1) process.nextTick(send);
    return emitter;
  };

  emitter.close = function() {
    if (queue.length) closing = 1;
    else udp.close();
    return emitter;
  };

  return emitter;
};

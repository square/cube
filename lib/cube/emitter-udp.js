var util = require("util"),
    dgram = require("dgram"),
    setImmediate = require("./set-immediate");

// returns an emitter which sneds events one at a time to the given udp://host:port
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
      if (queue.length) setImmediate(send);
      else if (closing) udp.close();
    });
  }

  emitter.send = function(event) {
    if (!closing && queue.push(event) == 1) setImmediate(send);
    return emitter;
  };

  emitter.close = function() {
    if (queue.length) closing = 1;
    else udp.close();
    return emitter;
  };

  return emitter;
};

var util = require("util"),
    http = require("http");

module.exports = function(protocol, host, port) {
  var emitter = {},
      queue = [],
      closing;

  if (protocol != "http:") throw new Error("invalid HTTP protocol");

  function send() {
    var events = queue.splice(0, 500);
    if (events.length === 0) return;

    var body = JSON.stringify(events);

    var postOptions = {
      host: host,
      port: port,
      path: "/1.0/event/put",
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': body.length
      }
    };

    var postRequest = http.request(postOptions, function(res) {
      if (res.statusCode !== 200) queue.unshift.apply(queue, events);
      if (queue.length) setTimeout(send, 500);
    });

    postRequest.on('error', function (e) {
      console.warn(e.message);
      queue.unshift.apply(queue, events);
    });

    postRequest.write(body);
    postRequest.end();
  }

  emitter.send = function(event) {
    if (!closing && queue.push(event) == 1) process.nextTick(send);
    return emitter;
  };

  emitter.close = function () {
    if (queue.length) closing = 1;
    return emitter;
  };

  return emitter;
};

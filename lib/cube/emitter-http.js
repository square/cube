var util = require("util"),
    http = require("http");

var batchSize = 500,      // events per batch
    batchInterval = 500,  // ms between batches
    errorInterval = 1000; // ms between retries

// returns an emitter which POSTs events up to 500 at a time to the given http://host:port
module.exports = function(protocol, host, port) {
  var emitter = {},
      queue = [],
      closing;

  if (protocol != "http:") throw new Error("invalid HTTP protocol");

  function send() {
    var events = queue.splice(0, batchSize),
        body = JSON.stringify(events);

    http.request({
      host: host,
      port: port,
      path: "/1.0/event",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": body.length
      }
    }, function(response) {
      if (response.statusCode !== 200) return error(response.statusCode);
      if (queue.length) setTimeout(send, batchInterval);
    }).on("error", function(e) {
      error(e.message);
    }).end(body);

    function error(message) {
      util.log("error: " + message);
      queue.unshift.apply(queue, events);
      setTimeout(send, errorInterval);
    }
  }

  emitter.send = function(event) {
    if (!closing && queue.push(event) === 1) setTimeout(send, batchInterval);
    return emitter;
  };

  emitter.close = function () {
    if (queue.length) closing = 1;
    return emitter;
  };

  return emitter;
};

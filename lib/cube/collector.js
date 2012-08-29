//
// collector -- listen for incoming metrics
//

var endpoint = require("./endpoint"),
    metalog  = require('./metalog');

var headers = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*"
};

// Register Collector listeners at their appropriate paths:
//
// * putter,   handles each isolated event -- see event.js
// * poster,   an HTTP listener -- see below
// * collectd, a collectd listener -- see collectd.js
//
exports.register = function(db, endpoints) {
  var putter = require("./event").putter(db),
      poster = post(putter);

  //
  endpoints.ws.push(
    endpoint("/1.0/event/put", putter)
  );

  //
  endpoints.http.push(
    endpoint("POST", "/1.0/event", poster),
    endpoint("POST", "/1.0/event/put", poster),
    endpoint("POST", "/collectd", require("./collectd").putter(putter))
  );

  //
  endpoints.udp = putter;
};

//
// Construct HTTP listener
//
// * aggregate content into a complete request
// * JSON-parse the request body
// * dispatch each metric as an event to the putter
//
function post(putter) {
  return function(request, response) {
    var content = "";
    request.on("data", function(chunk) {
      content += chunk;
    });
    request.on("end", function() {
      try {
        JSON.parse(content).forEach(putter);
      } catch (e) {
        metalog.event("cube_request", { at: "c", res: "collector_post_error", error: e, code: 400 });
        response.writeHead(400, headers);
        response.end(JSON.stringify({error: e.toString()}));
        return;
      }
      response.writeHead(200, headers);
      response.end("{}");
    });
  };
}

// ignore -- used for testing
exports._post = post;

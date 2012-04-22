var endpoint = require("./endpoint");

//
var headers = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*"
};

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
        response.writeHead(400, headers);
        response.end(JSON.stringify({error: e.toString()}));
        return;
      }
      response.writeHead(200, headers);
      response.end("{}");
    });
  };
}

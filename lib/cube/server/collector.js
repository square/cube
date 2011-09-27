var endpoint = require("./endpoint"),
    util = require("util");

exports.register = function(db, endpoints) {
  var putter = require("./event").putter(db);
  endpoints.ws.push(
    endpoint.exact("/1.0/event/put", putter)
  );
  endpoints.http.push(
    endpoint.exact("POST", "/1.0/event/put", post(putter)),
    endpoint.exact("POST", "/collectd", require("./collectd").putter(putter))
  );
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
        util.log(e);
        response.writeHead(400, {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        });
        return response.end("{\"status\":400}");
      }
      response.writeHead(200, {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*"
      });
      response.end("{\"status\":200}");
    });
  };
}

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
        var eventData = JSON.parse(content);
        // We'll accept both arrays and objects as the root value of the posted
        // JSON. If it has a defined forEach key, we'll assume it's an array. If
        // not, then we'll pass it to putter if it's an object, otherwise we'll
        // throw an error.
        if (eventData.forEach !== undefined) {
          eventData.forEach(putter);
        } else if (typeof(eventData) === "object") {
          putter(eventData);
        } else {
          var type = typeof(eventData);
          throw(new Error("I don't know what to do with a " + type));
        }
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

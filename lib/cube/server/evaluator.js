var endpoint = require("./endpoint"),
    url = require("url");

var headers = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*"
};

exports.register = function(db, endpoints) {
  var event = require("./event").getter(db),
      metric = require("./metric").getter(db);

  endpoints.ws.push(
    endpoint.exact("/1.0/event/get", event),
    endpoint.exact("/1.0/metric/get", metric)
  );
  endpoints.http.push(
    endpoint.exact("GET", "/1.0/metric", metricGet),
    endpoint.exact("GET", "/1.0/metric/get", metricGet)
  );

  function metricGet(request, response) {
    var values = [];

    if (metric(url.parse(request.url, true).query, callback) < 0) {
      response.writeHead(400, headers);
      response.end("[]");
    } else {
      response.writeHead(200, headers);
    }

    function callback(value) {
      if (value == null) response.end(JSON.stringify(values.sort(chronological)));
      else values.push(value);
    }
  }
};

function chronological(a, b) {
  return a.time - b.time;
}

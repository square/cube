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
    var values = [],
        expected = metric(url.parse(request.url, true).query, callback);

    response.writeHead(expected < 0 ? 400 : 200, headers);
    if (expected <= 0) response.end(JSON.stringify([]));

    function callback(value) {
      if (values.push(value) === expected) {
        response.end(JSON.stringify(values.sort(chronological)));
      }
    }
  }
};

function chronological(a, b) {
  return a.time - b.time;
}

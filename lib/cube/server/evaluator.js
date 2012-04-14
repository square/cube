var endpoint = require("./endpoint"),
    url = require("url");

var headers = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*"
};

exports.register = function(db, endpoints) {
  var event = require("./event").getter(db),
      metric = require("./metric").getter(db),
      info = require("./info").info;

  endpoints.ws.push(
    endpoint.exact("/1.0/event/get", event),
    endpoint.exact("/1.0/metric/get", metric)
  );
  endpoints.http.push(
    endpoint.exact("GET", "/1.0/metric", metricGet),
    endpoint.exact("GET", "/1.0/metric/get", metricGet),
    endpoint.exact("GET", "/1.0/info", infoGet)
  );

  function metricGet(request, response) {
    request = url.parse(request.url, true).query;
    var data = [], stop = new Date(request.stop);

    if (metric(request, callback) < 0) {
      response.writeHead(400, headers);
      response.end("[]");
    } else {
      response.writeHead(200, headers);
    }

    function callback(d) {
      if (d.time >= stop) response.end(JSON.stringify(data.sort(chronological)));
      else data.push(d);
    }
  }

  function infoGet(request, response) {
    response.writeHead(200, headers);
    response.end(JSON.stringify(info));
  }
};

function chronological(a, b) {
  return a.time - b.time;
}

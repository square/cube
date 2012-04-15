var endpoint = require("./endpoint"),
    url = require("url");

// To avoid running out of memory, the GET endpoints have a maximum number of
// values they can return. If the limit is exceeded, only the most recent
// results are returned.
var limit = 1e4;

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
    endpoint.exact("GET", "/1.0/event", eventGet),
    endpoint.exact("GET", "/1.0/event/get", eventGet),
    endpoint.exact("GET", "/1.0/metric", metricGet),
    endpoint.exact("GET", "/1.0/metric/get", metricGet)
  );

  function eventGet(request, response) {
    request = url.parse(request.url, true).query;
    var data = [];

    // A stop time is required for the REST API.
    if (!("stop" in request)) {
      response.writeHead(400, headers);
      response.end(JSON.stringify({error: "invalid stop"}));
      return;
    }

    if (event(request, callback) < 0) {
      response.writeHead(400, headers);
      response.end(JSON.stringify(data[0]));
    } else {
      response.writeHead(200, headers);
    }

    function callback(d) {
      if (d == null) response.end(JSON.stringify(data.reverse()));
      else if (data.push(d) >= limit) event.close(callback);
    }
  }

  function metricGet(request, response) {
    request = url.parse(request.url, true).query;
    var data = [],
        start = new Date(request.start),
        stop = new Date(request.stop),
        step = +request.step;

    // If the time between start and stop is too long, then bring the start time
    // forward so that only the most recent results are returned. This is only
    // approximate in the case of months, but why would you want to return
    // exactly ten thousand months? Don't rely on exact limits!
    if ((stop - start) / step > limit) request.start = new Date(stop - step * limit);

    if (metric(request, callback) < 0) {
      response.writeHead(400, headers);
      response.end(JSON.stringify(data[0]));
    } else {
      response.writeHead(200, headers);
    }

    function callback(d) {
      if (d.time >= stop) response.end(JSON.stringify(data.sort(chronological)));
      else data.push(d);
    }
  }
};

function chronological(a, b) {
  return a.time - b.time;
}

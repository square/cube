var options = require("../../../config/evaluator"),
    server = require("./server")(options),
    endpoint = require("./endpoint"),
    visualizer = require("./visualizer");

server.register = function(db, endpoints) {
  endpoints.ws.push(
    endpoint.exact("/1.0/event/get", require("./event").getter(db)),
    endpoint.exact("/1.0/metric/get", require("./metric").getter(db))
  );
  visualizer.register(db, endpoints);
};

server.start();

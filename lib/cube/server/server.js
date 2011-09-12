var util = require("util"),
    url = require("url"),
    websocket = require("websocket-server"),
    mongodb = require("mongodb");

// Don't crash on errors.
process.on("uncaughtException", function(error) {
  if (error.code !== "EPIPE") util.log(error.stack);
});

module.exports = function(options) {
  var server = {},
      socket = websocket.createServer(),
      endpoints = {ws: [], http: []},
      mongo = new mongodb.Server(options["mongo-host"], options["mongo-port"]),
      db = new mongodb.Db(options["mongo-database"], mongo);

  // Register WebSocket listener.
  socket.on("connection", function(connection) {
    util.log(connection._socket.remoteAddress + " " + connection._req.url);

    // Forward messages to the appropriate endpoint, or close the connection.
    for (var i = -1, n = endpoints.ws.length, e; ++i < n;) {
      if ((e = endpoints.ws[i]).match(connection._req.url)) {

        function callback(response) {
          connection.send(JSON.stringify(response));
        }

        callback.id = connection.id;

        // Listen for close events.
        if (e.dispatch.close) {
          connection.on("close", function() {
            interval = clearInterval(interval);
            e.dispatch.close(callback);
          });

          // Unfortunately, it looks like there is a bug in websocket-server (or
          // somewhere else) where close events are not emitted if the socket is
          // closed very shortly after it is opened. So we do an additional
          // check using an interval to verify that the socket is still open.
          var interval = setInterval(function() {
            if (!connection._socket.writable) {
              interval = clearInterval(interval);
              connection.close();
            }
          }, 5000);
        }

        return connection.on("message", function(request) {
          e.dispatch(JSON.parse(request), callback);
        });
      }
    }

    connection.close();
  });

  // Register HTTP listener.
  socket.on("request", function(request, response) {
    var u = url.parse(request.url);
    util.log(request.connection.remoteAddress + " " + u.pathname);

    // Forward messages to the appropriate endpoint, or 404.
    for (var i = -1, n = endpoints.http.length, e; ++i < n;) {
      if ((e = endpoints.http[i]).match(u.pathname, request.method)) {
        return e.dispatch(request, response);
      }
    }

    response.writeHead(404, {"Content-Type": "text/plain"});
    response.end("404 Not Found");
  });

  server.start = function() {
    // Connect to mongodb.
    util.log("starting mongodb client");
    db.open(function(error) {
      if (error) throw error;
      server.register(db, endpoints);
    });

    // Start the server!
    util.log("starting http server on port " + options["http-port"]);
    socket.listen(options["http-port"]);
  };

  return server;
};

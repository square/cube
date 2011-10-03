var util = require("util"),
    url = require("url"),
    http = require("http"),
    websocket = require("websocket"),
    websprocket = require("websocket-server"),
    mongodb = require("mongodb");

// Don't crash on errors.
process.on("uncaughtException", function(error) {
  if (error.code !== "EPIPE") util.log(error.stack);
});

// And then this happened:
websprocket.Connection = require("../../../node_modules/websocket-server/lib/ws/connection");

// Configuration for WebSocket requests.
var wsOptions =  {
  maxReceivedFrameSize: 0x10000,
  maxReceivedMessageSize: 0x100000,
  fragmentOutgoingMessages: true,
  fragmentationThreshold: 0x4000,
  keepalive: true,
  keepaliveInterval: 20000,
  assembleFragments: true,
  disableNagleAlgorithm: true,
  closeTimeout: 5000
};

module.exports = function(options) {
  var server = {},
      primary = http.createServer(),
      secondary = websprocket.createServer(),
      endpoints = {ws: [], http: []},
      mongo = new mongodb.Server(options["mongo-host"], options["mongo-port"]),
      db = new mongodb.Db(options["mongo-database"], mongo),
      id = 0;

  secondary.server = primary;

  // Register primary WebSocket listener with fallback.
  primary.on("upgrade", function(request, socket, head) {
    if ("sec-websocket-version" in request.headers) {
      request = new websocket.request(socket, request, wsOptions);
      request.readHandshake();
      connect(request.accept(request.requestedProtocols[0], request.origin), request.httpRequest);
    } else if (request.method === "GET"
        && /^websocket$/i.test(request.headers.upgrade)
        && /^upgrade$/i.test(request.headers.connection)) {
      new websprocket.Connection(secondary.manager, secondary.options, request, socket, head);
    }
  });

  // Register secondary WebSocket listener.
  secondary.on("connection", function(connection) {
    connection.socket = connection._socket;
    connection.remoteAddress = connection.socket.remoteAddress;
    connection.sendUTF = connection.send;
    connect(connection, connection._req);
  });

  function connect(connection, request) {
    util.log(connection.remoteAddress + " " + request.url);

    // Forward messages to the appropriate endpoint, or close the connection.
    for (var i = -1, n = endpoints.ws.length, e; ++i < n;) {
      if ((e = endpoints.ws[i]).match(request.url)) {

        function callback(response) {
          connection.sendUTF(JSON.stringify(response));
        }

        callback.id = ++id;

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
            if (!connection.socket.writable) {
              interval = clearInterval(interval);
              connection.close();
            }
          }, 5000);
        }

        return connection.on("message", function(request) {
          e.dispatch(JSON.parse(request.utf8Data || request), callback);
        });
      }
    }

    connection.close();
  }

  // Register HTTP listener.
  primary.on("request", function(request, response) {
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
    primary.listen(options["http-port"]);
  };

  return server;
};

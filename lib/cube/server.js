// Server -- generic HTTP, UDP and websockets server
//
// Used by the collector to accept new events via HTTP or websockets
// Used by the evaluator to serve pages over HTTP, and the continuously-updating
//   metrics stream over websockets
//
// holds
// * the primary and secondary websockets connections
// * the HTTP listener connection
// * the MongoDB connection
// * the UDP listener connection
//

var util = require("util"),
    url = require("url"),
    http = require("http"),
    dgram = require("dgram"),
    websocket = require("websocket"),
    websprocket = require("websocket-server"),
    static = require("node-static"),
    mongodb = require("mongodb"),
    authentication = require("./authentication"),
    metalog = require("./metalog");

// Don't crash on errors.
process.on("uncaughtException", function(error) {
  util.log("uncaught exception: " + error);
  util.log(error.stack);
});

// And then this happened:
websprocket.Connection = require("../../node_modules/websocket-server/lib/ws/connection");

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

// MongoDB driver configuration.
var server_options = {auto_reconnect: true},
    db_options = {native_parser: true};

module.exports = function(options) {
  var server = {},
      primary = http.createServer(),
      secondary = websprocket.createServer(),
      file = new static.Server("static"),
      endpoints = {ws: [], http: []},
      mongo = new mongodb.Server(options["mongo-host"], options["mongo-port"], server_options),
      db = new mongodb.Db(options["mongo-database"], mongo, db_options),
      id = 0,
      authenticator;

  secondary.server = primary;

  function is_sec_ws_initiation(request){
    return ("sec-websocket-version" in request.headers);
  }
  function is_ws_initiation(request){
    return (request.method === "GET"
            && /^websocket$/i.test(request.headers.upgrade)
            && /^upgrade$/i.test(request.headers.connection));
  }

  // Register primary WebSocket listener with fallback.
  primary.on("upgrade", function(request, socket, head) {
    function auth_ok(perms) {
      if (is_sec_ws_initiation(request)) {
        request = new websocket.request(socket, request, wsOptions);
        request.readHandshake();
        connect(request.accept(request.requestedProtocols[0], request.origin), request.httpRequest);
      } else if (is_ws_initiation(request)) {
        new websprocket.Connection(secondary.manager, secondary.options, request, socket, head);
      }
    }
    function auth_no(perms) {
      if (is_sec_ws_initiation(request)) {
        request = new websocket.request(socket, request, wsOptions);
        request.readHandshake();
        request.reject();
      } else if (is_ws_initiation(request)) {
        res = 'HTTP/1.1 403 Forbidden\r\nConnection: close';
        socket.end(res + '\r\n\r\n', 'ascii');
      }
    }
    return authenticator.check(request, auth_ok, auth_no);
  });

  // Register secondary WebSocket listener.
  secondary.on("connection", function(connection) {
    connection.socket = connection._socket;
    connection.remoteAddress = connection.socket.remoteAddress;
    connection.sendUTF = connection.send;
    connect(connection, connection._req);
  });

  function connect(connection, request) {
    // save auth from connection requesta
    var authorization = request.authorized;

    // Forward messages to the appropriate endpoint, or close the connection.
    for (var i = -1, n = endpoints.ws.length, e; ++i < n;) {
      if ((e = endpoints.ws[i]).match(request.url)) {

        function callback(response) {
          connection.sendUTF(JSON.stringify(response));
        }

        callback.id = ++id;

        // Listen for socket disconnect.
        if (e.dispatch.close) connection.socket.on("end", function() {
          e.dispatch.close(callback);
        });

        connection.on("message", function(message) {
          // staple the authorization back on
          var payload = JSON.parse(message.utf8Data || message);
          payload.authorized = authorization;
          e.dispatch(payload, callback);
        });

        metalog.event('cube_request', { is: 'ws', method: "WebSocket", ip: connection.remoteAddress, path: request.url}, 'minor');
        return;
      }
    }
    connection.close();
  }

  // Register HTTP listener.
  primary.on("request", function(request, response) {
    var u = url.parse(request.url);

    // Forward messages to the appropriate endpoint, or 404.
    for (var i = -1, n = endpoints.http.length, e; ++i < n;) {
      if ((e = endpoints.http[i]).match(u.pathname, request.method)) {

        function auth_ok(perms) {
          metalog.event('cube_request', { is: 'auth_ok', method: request.method, ip: request.connection.remoteAddress, path: u.pathname, auth: true, user: perms });
          e.dispatch(request, response);
        }
        function auth_no(reason) {
          metalog.event('cube_request', { is: 'auth_no', method: request.method, ip: request.connection.remoteAddress, path: u.pathname, auth: false });
          response.writeHead(403, {"Content-Type": "text/plain"});
          response.end("403 Forbidden");
        }

        return authenticator.check(request, auth_ok, auth_no);
      }
    }

    // If this request wasn't matched, see if there's a static file to serve.
    request.on("end", function() {
      file.serve(request, response, function(error) {
        if (error) {
          metalog.event('cube_request', { is: 'failed', msg: error, code: error.status, ip: request.connection.remoteAddress, path: u.pathname });
          response.writeHead(error.status, {"Content-Type": "text/plain"});
          response.end(error.status + "");
        }
      });
    });
  });

  server.start = function() {
    // Connect to mongodb.
    mongo_password = options["mongo-password"]; delete options["mongo-password"];
    metalog.info('cube_life', {is: 'mongo_connect', options: options });
    db.open(function(error) {
      if (error) throw error;
      if (options["mongo-username"] == null) return ready();
      db.authenticate(options["mongo-username"], mongo_password, function(error, success) {
        if (error) throw error;
        if (!success) throw new Error("authentication failed");
        ready();
      });
    });

    // Start the server!
    function ready() {
      server.register(db, endpoints);
      metalog.putter = require("./event").putter(db);
      authenticator  = authentication.authenticator(options["authenticator"], db, options);
      metalog.event("cube_life", { is: 'start_http', port: options["http-port"] });
      primary.listen(options["http-port"]);
      if (endpoints.udp) {
        metalog.event("cube_life", { is: 'start_udp', port: options["udp-port"] });
        var udp = dgram.createSocket("udp4");
        udp.on("message", function(message) {
          endpoints.udp(JSON.parse(message.toString("utf8")), ignore);
        });
        udp.bind(options["udp-port"]);
      }
    }
  };

  return server;
};

function ignore() {
  // Responses for UDP are ignored; there's nowhere for them to go!
}

'use strict';

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

var util           = require("util"),
    url            = require("url"),
    http           = require("http"),
    dgram          = require("dgram"),
    websocket      = require("websocket"),
    websprocket    = require("websocket-server"),
    file_server    = require("node-static"),
    authentication = require("./authentication"),
    event          = require("./event"),
    metalog        = require("./metalog"),
    Db             = require("./db");

// Don't crash on errors.
process.on("uncaughtException", function(error) {
  metalog.error('server', error);
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

module.exports = function(options, db) {
  var server = {},
      primary = http.createServer(),
      secondary = websprocket.createServer(),
      file = new file_server.Server("static"),
      udp,
      endpoints = {ws: [], http: []},
      id = 0,
      authenticator;

  // allows dependency injection from test_helper
  if (! db) db = new Db();
  
  secondary.server = primary;

  function is_sec_ws_initiation(request){
    return ("sec-websocket-version" in request.headers);
  }
  function is_ws_initiation(request){
    return (request.method === "GET"                     &&
            (/^websocket$/i).test(request.headers.upgrade) &&
            (/^upgrade$/i).test(request.headers.connection) );
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

    function connection_callback(response) {
      metalog.dump_trace('resp', response); delete response._trace;
      connection.sendUTF(JSON.stringify(response));
    }

    // Forward messages to the appropriate endpoint, or close the connection.
    for (var i = -1, n = endpoints.ws.length, e; ++i < n;) {
      if ((e = endpoints.ws[i]).match(request.url)) {

        connection_callback.id = ++id;

        // Listen for socket disconnect.
        if (e.dispatch.close) connection.socket.on("end", function() {
          e.dispatch.close(connection_callback);
        });

        connection.on("message", function(message) {
          // parse, staple the authorization on, then process
          var payload = JSON.parse(message.utf8Data || message);
          payload.authorized = authorization;
          metalog.trace('req', payload);
          e.dispatch(payload, connection_callback);
        });

        metalog.event('connect', { method: 'ws', ip: connection.remoteAddress, path: request.url}, 'minor');
        return;
      }
    }
    connection.close();
  }

  // Register HTTP listener.
  primary.on("request", function(request, response) {
    var u = url.parse(request.url);
    metalog.trace('http', request);

    function auth_ok(perms) {
      metalog.trace('auth_ok', request, { method: request.method, ip: request.connection.remoteAddress, path: u.pathname });
      e.dispatch(request, response);
    }
    function auth_no(reason) {
      metalog.dump_trace('auth_no', request, { method: request.method, ip: request.connection.remoteAddress, path: u.pathname });
      response.writeHead(403, {"Content-Type": "text/plain"});
      response.end("403 Forbidden");
    }

    // Forward messages to the appropriate endpoint, or 404.
    for (var i = -1, n = endpoints.http.length, e; ++i < n;) {
      if ((e = endpoints.http[i]).match(u.pathname, request.method)) {
        return authenticator.check(request, auth_ok, auth_no);
      }
    }

    // If this request wasn't matched, see if there's a static file to serve.
    request.on("end", function() {
      file.serve(request, response, function(error) {
        if (error) {
          metalog.error('req_file', error, { ip: request.connection.remoteAddress, path: u.pathname });
          response.writeHead(error.status, {"Content-Type": "text/plain"});
          response.end(error.status + "");
        } else {
          metalog.trace('req_file', request, { ip: request.connection.remoteAddress, path: u.pathname });
        }
      });
    });

    // as of node v0.10, 'end' is not emitted unless read() called
    if (request.read !== undefined) {
      request.read();
    }
  });

  server.start = function(server_start_cb) {
    db.open(options, function(error, db){
      handle(error);
      ready(db);
    });

    // Start the server!
    function ready(db) {
      metalog.putter = event.putter(db);
      server.register(db, endpoints);
      authenticator  = authentication.authenticator(options["authenticator"], db, options);
      metalog.event('start_http', { port: options["http-port"] });
      primary.listen(options["http-port"]);
      if (endpoints.udp) {
        metalog.event('start_udp', { port: options["udp-port"] });
        udp = dgram.createSocket("udp4");
        udp.on("message", function(message) {
          endpoints.udp(JSON.parse(message.toString("utf8")), ignore);
        });
        udp.bind(options["udp-port"]);
      }
      if (server_start_cb) server_start_cb(null, options);
    }
  };

  primary.on(  "close", function(){ metalog.info('http_close'); });
  secondary.on("close", function(){ metalog.info('ws_close'  ); });
  function try_close(name, obj){ if (obj){
    try {
      metalog.info(name+'_stopping', options);
      obj.close( function(){ metalog.info(name+'_stop'); } );
    } catch(error){}
  } }
  server.stop = function(cb){
    try_close('http', primary);                   // stop serving
    try_close('ws',   secondary);
    try_close('udp',  udp);
    setTimeout(function(){try_close('mongo', db);}, 50); // stop db'ing
    if (cb) cb();
  };

  return server;
};

function ignore() {
  // Responses for UDP are ignored; there's nowhere for them to go!
}

function handle(error) {
  if (!error) return;
  metalog.error('server', error);
  throw error;
}

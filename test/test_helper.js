'use strict';

var assert      = require("assert"),
    http        = require("http"),
    dgram       = require('dgram'),
    mongodb     = require("mongodb"),
    metalog     = require("../lib/cube/metalog");

// ==========================================================================
//
// setup
//

var test_helper = {};
var test_db     = {};
var test_collections   = ["test_users", "test_events", "test_metrics"];
test_helper.inspectify = metalog.inspectify;

test_helper.settings = {
  "mongo-host":     "localhost",
  "mongo-port":     27017,
  "mongo-username": null,
  "mongo-password": null,
  "mongo-database": "cube_test",
  "host":           "localhost",
  "authenticator":  "allow_all"
};

// Disable logging for tests.
metalog.loggers.info  = metalog.silent; // log
metalog.loggers.minor = metalog.silent; // log
metalog.send_events = false;

// ==========================================================================
//
// client / server helpers
//

var port = 1083;
// test_helper.get_port() -- get a port ID, unique to your batch.
test_helper.get_port = function(){ return ++port; };

// test_helper.request -- make an HTTP request.
//
// @param    options standard http client options, with these defaults:
//   @option host    host to contact; default "localhost"
//   @option port    port to contact; detault `this.http_port`
// @param    data    request body
//
test_helper.request = function(options, data) {
  return function() {
    var cb = this.callback;

    options.host = "localhost";
    if (! options.port){ options.port = this.http_port; }

    var request = http.request(options, function(response) {
      response.body = "";
      response.setEncoding("utf8");
      response.on("data", function(chunk) { response.body += chunk; });
      response.on("end",  function() { cb(null, response); });
    });

    request.on("error", function(e) { cb(e, null); });

    if (data && data.length > 0) request.write(data);
    request.end();
  };
};

// send udp packet, twiddle thumbs briefly, resume tests.
test_helper.udp_request = function (data){
  return function(){
    var udp_client = dgram.createSocket('udp4');
    var buffer     = new Buffer(JSON.stringify(data));
    var context    = this;
    metalog.info('sending_udp', {  data: data });
    udp_client.send(buffer, 0, buffer.length, context.udp_port, 'localhost',
        function(err, val){ delayed_callback(context)(err, val); udp_client.close(); } );
  };
};

// proxies to the test context's callback after a short delay.
//
// @example as a test topic; will get the same data the cb otherwise would have:
//   { topic: send_some_data,
//     'a short time later': {
//       topic: test_helper.delaying_topic,
//       'is party time': function(arg){ assert.isAwesome(...) } } }
//
function delaying_topic(){
  var args = Array.prototype.slice.apply(arguments);
  args.unshift(null);
  delayed_callback(this).apply(this, args);
}
test_helper.delaying_topic = delaying_topic;

// returns a callback that once triggered, delays briefly, then passes the same
// args to the actual context's callback
//
// @example
//    // you
//    dcb = delayed_callback(this)
//    foo.do_something('...', dcb);
//    // foo, after do_something'ing, invokes the delayed callback
//    dcb(null, 1, 2);
//    // 50ms later, dcb does the equivalent of
//    this.callback(null, 1, 2);
//
function delayed_callback(context){
  return function(){
    var callback_delay = 100;
    var args = arguments;
    setTimeout(function(){ context.callback.apply(context, args); }, callback_delay);
  };
}
test_helper.delayed_callback = delayed_callback;

// test_helper.with_server --
//   start server, run tests once server starts, stop server when tests are done
//
// inscribes 'server', 'udp_port' and 'http_port' on the test context -- letting
// you say 'this.server' in your topics, etc.
//
// @param options    -- overrides for the settings, above.
// @param components -- passed to server.register()
// @param batch      -- the tests to run
test_helper.with_server = function(options, components, batch){
  return { '': {
    topic:    function(){ start_server(options, components, this);  },
    '':       batch,
    teardown: function(svr){ this.server.stop(this.callback); }
  } };
};

// @see test_helper.with_server
function start_server(options, register, vow){
  for (var key in test_helper.settings){
    if (! options[key]){ options[key] = test_helper.settings[key]; }
  }
  vow.http_port = options['http-port'];
  vow.udp_port  = options['udp-port'];
  vow.server = require('../lib/cube/server')(options);
  vow.server.register = register;
  vow.server.start(vow.callback);
}

// ==========================================================================
//
// db helpers
//

// test_helper.batch --
// * connect to db, drop relevant collections
// * run tests once db is ready;
// * close db when tests are done
test_helper.batch = function(batch) {
  return {
    "": {
      topic: function() {
        connect(test_helper.settings);
        setup_db(this.callback);
      },
      "": batch,
      teardown: function(test_db) {
        if (test_db.client.isConnected()) {
          process.nextTick(function(){ test_db.client.close(); });
        }
      }
    }
  };
};

// test_db.using_objects -- scaffold fixtures into the database, run tests once loaded.
//
// Wrap your tests in test_helper.batch to get the test_db object.
test_db.using_objects = function (clxn_name, test_objects, context){
  metalog.minor('cube_testdb', {state: 'loading test objects', test_objects: test_objects });
  test_db.db.collection(clxn_name, function(err, clxn){
    if (err) throw(err);
    context[clxn_name] = clxn;
    clxn.remove({ dummy: true }, {safe: true}, function(){
      clxn.insert(test_objects, { safe: true }, function(){
        context.callback(null, test_db);
      }); });
  });
};

// ==========================================================================
//
// db methods
//

// @see test_helper.batch
function setup_db(cb){
  drop_collections(cb);
}

// @see test_helper.batch
function connect(options){
  metalog.minor('cube_testdb', { state: 'connecting to db', options: options });
  test_db.options = options;
  test_db.client  = new mongodb.Server(options["mongo-host"], options["mongo-port"], {auto_reconnect: false});
  test_db.db      = new mongodb.Db(options["mongo-database"], test_db.client, {});
}

// @see test_helper.batch
function drop_collections(cb){
  metalog.minor('cube_testdb', { state: 'dropping test collections', collections: test_collections });
  test_db.db.open(function(error) {
    var collectionsRemaining = test_collections.length;
    test_collections.forEach(function(collection_name){
      test_db.db.dropCollection(collection_name,  collectionReady);
    });
    function collectionReady() {
      if (!--collectionsRemaining) {
        cb(null, test_db);
      }
    }
  });
}

// ==========================================================================
//
// fin.
//

module.exports = test_helper;

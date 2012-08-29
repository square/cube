var assert      = require("assert"),
    http        = require("http"),
    mongodb     = require("mongodb"),
    metalog     = require("../lib/cube/metalog");

// ==========================================================================
//
// setup
//

var test_helper = {}
var test_db     = {};
var test_collections   = ["test_users", "test_events", "test_metrics"];
test_helper.inspectify = metalog.inspectify;

test_helper.settings = {
  "mongo-host":     "localhost",
  "mongo-port":     27017,
  "mongo-username": null,
  "mongo-password": null,
  "mongo-database": "cube_test",
  "authenticator":  "allow_all"
}

// Disable logging for tests.
metalog.loggers.info  = metalog.silent;
metalog.loggers.minor = metalog.silent;
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
    if (! options.port){ options.port = this.http_port };

    var request = http.request(options, function(response) {
      response.body = "";
      response.setEncoding("utf8");
      response.on("data", function(chunk) { response.body += chunk; });
      response.on("end", function() { cb(null, response); });
    });

    request.on("error", function(e) { cb(e, null); });

    if (data && data.length > 0) request.write(data);
    request.end();
  };
};

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
    topic:    function(){ start_server(options, components, this); },
    '':       batch,
    teardown: function(svr){ this.server.stop(this.callback); }
  } }
}

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
        };
      }
    }
  };
};

// test_db.using_objects -- scaffold fixtures into the database, run tests once loaded.
//
// Wrap your tests in test_helper.batch to get the test_db object.
test_db.using_objects = function (clxn_name, test_objects, that){
  metalog.minor('cube_testdb', {state: 'loading test objects', test_objects: test_objects });
  test_db.db.collection(clxn_name, function(err, clxn){
    if (err) throw(err);
    that[clxn_name] = clxn;
    clxn.remove({ dummy: true }, {safe: true}, function(){
      clxn.insert(test_objects, { safe: true }, function(){
        that.callback(null, test_db);
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
  test_db.client  = new mongodb.Server(options["mongo-host"], options["mongo-port"], {auto_reconnect: true});
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

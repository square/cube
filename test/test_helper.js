'use strict';

var _ = require("underscore"),
    util        = require("util"),
    assert      = require("assert"),
    http        = require("http"),
    dgram       = require('dgram'),
    Db          = require("../lib/cube/db"),
    metalog     = require("../lib/cube/metalog"),
    config      = require("../config/cube");

// ==========================================================================
//
// setup
//

var test_helper = {};
var test_collections   = ["test_users", "test_events", "test_metrics", "test_boards"];
test_helper.inspectify = metalog.inspectify;
test_helper._          = require('underscore');

config.set('mongodb', {
  'mongo-host': 'localhost',
  'mongo-port': 27017,
  'mongo-username': null,
  'mongo-password': null,
  'mongo-database': 'cube_test',
  'host': 'localhost',
  'authentication-collection': 'test_users'
});

config.set('horizons', {
  calculation: +(new Date()),
  invalidation: +(new Date())
});

var basePort = 1083;
config.set('collector', {
  'http-port': basePort++,
  'udp-port': basePort++,
  'authenticator': 'allow_all'
});

config.set('evaluator', {
  'http-port': basePort++,
  'authenticator': 'allow_all'
});

config.set('warmer', {
  'warmer-interval': 10000,
  'warmer-tier': 10000
});


// Disable logging for tests.
metalog.loggers.info  = metalog.silent; // log
metalog.loggers.minor = metalog.silent; // log
metalog.send_events = false;

// ==========================================================================
//
// client / server helpers
//

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
    var ctxt       = this, cb = ctxt.callback;
    metalog.info('test_sending_udp', {  data: data });
    udp_client.send(buffer, 0, buffer.length, ctxt.udp_port, 'localhost',
        function(err, val){ delay(cb, ctxt)(err, val); udp_client.close(); } );
  };
};

// proxies to the test context's callback after a short delay.
//
// @example the test topic introduces a delay; the 'is party time' vow gets the same data the cb otherwise would have:
//   { topic: send_some_data,
//     'a short time later': {
//       topic: test_helper.delaying_topic,
//       'is party time': function(arg){ assert.isAwesome(...) } } }
//
function delaying_topic(){
  var args = Array.prototype.slice.apply(arguments);
  args.unshift(null);
  delay(this.callback, this).apply(this, args);
}
test_helper.delaying_topic = delaying_topic;

// returns a callback that once triggered, delays briefly, then passes the same
// args to the actual context's callback
//
// @example
//    // you
//    dcb = delay(this)
//    foo.do_something('...', dcb);
//    // foo, after do_something'ing, invokes the delayed callback
//    dcb(null, 1, 2);
//    // 50ms later, dcb does the equivalent of
//    this.callback(null, 1, 2);
//
function delay(orig_cb, ctxt, ms){
  ctxt = ctxt || null;
  ms   = ms   || 100;
  return function(){
    var args = arguments;
    setTimeout(function(){ orig_cb.apply(ctxt, args); }, ms);
  };
}
test_helper.delay = delay;

// test_helper.with_server --
//   start server, run tests once server starts, stop server when tests are done
//
// inscribes 'server', 'udp_port' and 'http_port' on the test context -- letting
// you say 'this.server' in your topics, etc.
//
// @param kind       -- types of server to run.
// @param components -- passed to server.register()
// @param batch      -- the tests to run
test_helper.with_server = function(kind, components, batch){
  return test_helper.batch({ '': {
    topic:    function(test_db){
      var ctxt = this, cb = ctxt.callback;
      start_server(kind, components, ctxt, test_db);
    },
    '':       batch,
    teardown: function(j_, test_db){
      var callback = this.callback;
      this.server.stop(function(){
        metalog.info('test_server_batch_closed');
        callback();
      });
    }
  } });
};

// @see test_helper.with_server
function start_server(kind, register, ctxt, test_db){
  var config = require('../config/cube').get(kind);
  ctxt.http_port = config['http-port'];
  ctxt.udp_port  = config['udp-port'];
  ctxt.server = require('../lib/cube/server')(kind, test_db);
  ctxt.server.use(register);
  ctxt.server.start(ctxt.callback);
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
  metalog.info('batch', batch);
  return {
    "": {
      topic: function() {
        var ctxt = this;
        ctxt.db = new Db();
        ctxt.db.open(function(error){
          drop_and_reopen_collections(ctxt.db, function(error){
            ctxt.callback.apply(ctxt, arguments);
            ctxt.db.clearCache();
          });
        });
      },
      "": batch,
      teardown: function(){
        var callback = this.callback;
        this.db.close(function(){
          metalog.info('test_db_batch_closed');
          callback();
        });
      }
    }
  };
};

// test_db.using_objects -- scaffold fixtures into the database, run tests once loaded.
//
// Wrap your tests in test_helper.batch to get the test_db object.
Db.prototype.using_objects = function (clxn_name, test_objects, ctxt){
  var test_db = this;
  metalog.minor('test_db_loading_objects', test_objects);
  test_db.collection(clxn_name, function(err, clxn){
    if (err) throw(err);
    ctxt[clxn_name] = clxn;
    clxn.remove({ dummy: true }, function(){
      clxn.insert(test_objects, function(){
        ctxt.callback(null, test_db);
      }); });
  });
};

// ==========================================================================
//
// db methods
//

// @see test_helper.batch
function drop_and_reopen_collections(test_db, cb){
  metalog.minor('test_db_drop_collections', { collections: test_collections });

  var collectionsRemaining = test_collections.length;
  test_collections.forEach(function(collection_name){
    test_db.collection(collection_name, function(error, collection){
      collection.drop(collectionReady);
    })
  });
  function collectionReady() {
    if (!--collectionsRemaining) {
      cb(null, test_db);
    }
  }
}

// ==========================================================================
//
// assertions
//

assert.isCalledTimes = function(ctxt, reps){
  var results = [], finished = false;
  setTimeout(function(){ if (! finished){ ctxt.callback(new Error('timeout: need '+reps+' results only have '+util.inspect(results))); } }, 2000);
  return function _is_called_checker(){
    results.push(_.toArray(arguments));
    if (results.length >= reps){ finished = true; ctxt.callback(null, results); }
  };
};

assert.isNotCalled = function(name){
  return function(){ throw new Error(name + ' should not have been called, but was'); };
};

// ==========================================================================
//
// fin.
//

module.exports = test_helper;

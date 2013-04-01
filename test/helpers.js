var database = require("../lib/cube/database"),
    util = require("util"),
    http = require("http");

var config = exports.config = require('./test-config');

exports.batch = function(batch) {
  return {
    "": {
      topic: function() {
        var cb = this.callback;
        database.open(config, function(error, db) {
          if (error) {
            return cb(error);
          }
          var collectionsRemaining = 2;
          db.dropCollection("test_events", collectionReady);
          db.dropCollection("test_metrics", collectionReady);
          function collectionReady() {
            if (!--collectionsRemaining) {
              cb(null, {db: db});
            }
          }
        });
      },
      "": batch,
      teardown: function(test) {
        test.db.close();
      }
    }
  };
};

exports.request = function(options, data) {
  return function() {
    var cb = this.callback;

    options.host = "localhost";

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

// Disable logging for tests.
util.log = function() {};

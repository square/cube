var mongodb = require("mongodb"),
    util = require("util"),
    http = require("http");

exports.port = 1083;

exports.batch = function(batch) {
  return {
    "": {
      topic: function() {
        var client = new mongodb.Server("localhost", 27017),
            db = new mongodb.Db("cube_test", client, { safe: false }),
            cb = this.callback;
        db.open(function(error) {
          if (error) {
            return cb(error);
          }
          var collectionsRemaining = 2;
          db.dropCollection("test_events", collectionReady);
          db.dropCollection("test_metrics", collectionReady);
          function collectionReady() {
            if (!--collectionsRemaining) {
              cb(null, {client: client, db: db});
            }
          }
        });
      },
      "": batch,
      teardown: function(test) {
        if (test.client.isConnected()) {
          test.client.close();
        }
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

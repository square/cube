var assert      = require("assert"),
    http        = require("http"),
    metalog     = require("../lib/cube/metalog"),
    test_db     = require("./test_db");

exports.port  = 1083;
exports.batch = test_db.batch;

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
metalog.loggers.info  = metalog.silent;
metalog.loggers.minor = metalog.silent;
metalog.send_events = false;

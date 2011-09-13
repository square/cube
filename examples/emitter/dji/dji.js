var util = require("util"),
    emitter = require("../../../lib/cube/server/emitter"),
    options = require("./dji-config");

// Connect to websocket.
util.log("starting websocket client");
var client = emitter().open(options["http-host"], options["http-port"]);

// Emit stock data.
readline(function(line, i) {
  if (i) {
    var fields = line.split(",");
    client.send({
      type: "stock",
      time: new Date(fields[0]),
      data: {
        open: +fields[1],
        high: +fields[2],
        low: +fields[3],
        close: +fields[4],
        volume: +fields[5]
      }
    });
  }
});

function readline(callback) {
  var stdin = process.openStdin(), line = "", i = -1;
  stdin.setEncoding("utf8");
  stdin.on("data", function(string) {
    var lines = string.split("\n");
    lines[0] = line + lines[0];
    line = lines.pop();
    lines.forEach(function(line) { callback(line, ++i); });
  });
  stdin.on("end", function() {
    util.log("stopping websocket client");
    client.close();
  });
}

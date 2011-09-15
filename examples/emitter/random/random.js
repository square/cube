var util = require("util"),
    cube = require("../../../"),
    options = require("./random-config"),
    count = 0,
    batch = 10,
    hour = 60 * 60 * 1000,
    start = Date.now(),
    offset = -Math.abs(random()) * 24 * hour;

// Connect to websocket.
util.log("starting websocket client");
var client = cube.emitter().open(options["http-host"], options["http-port"]);

// Emit random values.
var interval = setInterval(function() {
  for (var i = -1; ++i < batch;) {
    client.send({
      type: "random",
      time: new Date(Date.now() + random() * 2 * hour + offset),
      data: {random: (offset & 1 ? 1 : -1) * Math.random()}
    });
    count++;
  }
  var duration = Date.now() - start;
  console.log(count + " events in " + duration + " ms: " + Math.round(1000 * count / duration) + " sps");
}, 10);

// Display stats on shutdown.
process.on("SIGINT", function() {
  console.log("stopping websocket client");
  client.close();
  clearInterval(interval);
});

// Sample from a normal distribution with mean 0, stddev 1.
function random() {
  var x = 0, y = 0, r;
  do {
    x = Math.random() * 2 - 1;
    y = Math.random() * 2 - 1;
    r = x * x + y * y;
  } while (!r || r > 1);
  return x * Math.sqrt(-2 * Math.log(r) / r);
}

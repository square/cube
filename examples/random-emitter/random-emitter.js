process.env.TZ = 'UTC';

var util = require("util"),
    cube = require("../../"), // replace with require("cube")
    options = require("./random-config");

util.log("starting emitter");
var emitter = cube.emitter(options["collector"]);

var start = Date.now() + options["offset"],
    stop = start + options["duration"],
    step = options["step"],
    value = 0,
    count = 0;

while (start < stop) {
  emitter.send({
    type: "random",
    time: new Date(start),
    data: {
      value: value += Math.random() - .5
    }
  });
  start += step;
  ++count;
}

util.log("sent " + count + " events");
util.log("stopping emitter");
emitter.close();

process.env.TZ = 'UTC';

var cube       = require("../../"), // replace with require("cube")
    metalog    = cube.metalog,
    options    = require("./random-config"),
    cromulator = require("./cromulator"),
    models     = require("../../lib/cube/models"), Event = models.Event;

var options = {
  "collector": "ws://127.0.0.1:1080",

  // The offset and duration to backfill, in milliseconds.
  // For example, if the offset is minus four hours, then the first event that
  // the random emitter sends will be four hours old. It will then generate more
  // recent events based on the step interval, all the way up to the duration.
  "offset":   -0.49 * 60 * 60 * 1000,
  "duration":  0.49 * 60 * 60 * 1000,

  // The time between random events.
  "step": 1000 * 2,

  event_type: "doh"
};

var emitter = cube.emitter(options["collector"]);

cromulator.start = Date.now()       + options.offset;
cromulator.stop  = cromulator.start + options.duration;
cromulator.step  = options.step;

metalog.info('emitter', cromulator.report('starting'));

var time = cromulator.start;
while (time < cromulator.stop) {
  var event = new Event(options.event_type, cromulator.spread_time(time), cromulator.data_at(time));
  if (cromulator.count % 1000 == 0) metalog.info('emitter', {em: emitter.report(), cr: cromulator.report('progress', time)});
  event.force = true;
  emitter.send(event.to_request());
  time += cromulator.step;
}

metalog.info('emitter', cromulator.report('stopping', time));
emitter.close();

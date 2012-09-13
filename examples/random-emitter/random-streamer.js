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

  "event_frequency": 5000, // per second
  event_type: "doh"
};

var emitter = cube.emitter(options["collector"]);

metalog.info('emitter', cromulator.report('starting'));

function send(){
  cromulator.start = new Date(Math.floor(new Date() / (1000 * 60 * 60 * 24)) * 1000 * 60 * 60 * 24);
  cromulator.stop  = new Date(+cromulator.start + (1000 * 60 * 60 * 24));
  cromulator.step  = 1000 / options.event_frequency;

  var base_time = new Date(Math.floor(new Date() / 1000) * 1000),
      step      = 1000 / options.event_frequency,
      i         = 0;

  while (i < options.event_frequency) {
    var time  = new Date(+base_time + (step * i)),
        event = new Event(options.event_type, time, cromulator.data_at(time));
    if (i % 1000 == 0) metalog.info('emitter', {em: emitter.report(), cr: cromulator.report('progress', time)});
    event.force = true;
    emitter.send(event.to_request());
    i = i + 1;
  }
}

setInterval(send, 1000);

//metalog.info('emitter', cromulator.report('stopping', new Date()));
//emitter.close();

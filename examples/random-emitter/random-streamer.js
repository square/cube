process.env.TZ = 'UTC';

var cube       = require("../../"), // replace with require("cube")
    metalog    = cube.metalog,
    options    = require("./random-config"),
    cromulator = require("./cromulator"),
    Event      = require("../../lib/cube/models/event");

var options = {
  "collector": "ws://127.0.0.1:6000",

  // The offset and duration to backfill, in milliseconds.
  // For example, if the offset is minus four hours, then the first event that
  // the random emitter sends will be four hours old. It will then generate more
  // recent events based on the step interval, all the way up to the duration.

  "event_frequency": 5000, // per second
  event_batch: 500,
  event_type: "doh"
};

var emitter = cube.emitter(options["collector"]),
    step    = 1000 / options.event_frequency,
    batch   = options.event_batch || (step < 1 ? (1 / step) : 1);


function setup_cromulator(){
  var day = 1000 * 60 * 60 * 24;
  cromulator.start = new Date(Math.floor(new Date() / day) * day);
  cromulator.stop  = new Date(+cromulator.start + day);
  cromulator.step  = step;
 }

metalog.info('emitter', cromulator.report('starting'));

function send(){
  if (!cromulator.stop || +cromulator.stop <= +(new Date())) setup_cromulator();
  var i = -1;

  while (++i < batch) {
    var time  = new Date(),
        event = new Event(options.event_type, time, cromulator.data_at(time));
    if (cromulator.count % options.event_frequency == 0) metalog.info('emitter', {em: emitter.report(), cr: cromulator.report('progress', time)});
    event.force = true;
    emitter.send(event.to_request());
  }
}

var interval = 1000 / (options.event_frequency / batch);
setInterval(send, interval);
module.exports = {

  // The collector to send events to.
  "collector": "ws://127.0.0.1:1080",

  // The offset and duration to backfill, in milliseconds.
  // For example, if the offset is minus four hours, then the first event that
  // the random emitter sends will be four hours old. It will then generate more
  // recent events based on the step interval, all the way up to the duration.
  "offset": -4 * 60 * 60 * 1000,
  "duration": 8 * 60 * 60 * 1000,

  // The time between random events.
  "step": 1000 * 10
};

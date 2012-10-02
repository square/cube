'use strict';

var metalog = require('../metalog'),
    Model   = require('../core_ext/model'),
    reduces = require('../reduces'),
    Metric  = require('./metric'),
    Event   = require('./event'),
    compute = {constant: constant, binary: binary, unary: unary},
    queueByName  = {};

function Measurement(expression, start, stop, tier){
  // Round the start/stop to the tier edges
  this.expression = expression;
  this.start      = start;
  this.stop       = stop;
  this.tier       = tier;
  this.flavor     = (expression.op ? 'binary' : (expression.type ? 'unary' : 'constant'));

  this.eventize();
}

Model.modelize(Measurement);

Measurement.prototype.report = function report(){
  return { flavor: this.flavor, tier: this.tier.key, start: this.tier.bin(this.start), stop: this.tier.bin(this.stop), expr: (this.expression.op||this.expression.source||this.expression.value()) };
};

// Computes the metric for the given expression for the time interval from
// start (inclusive) to stop (exclusive). The time granularity is determined
// by the specified tier, such as daily or hourly. The callback is invoked
// repeatedly for each metric value, being passed two arguments: the time and
// the value. The values may be out of order due to partial cache hits.
Measurement.prototype.measure = function measure(db, callback) {
  var _this = this;

  compute[this.flavor].call(this, db, function(time, value){ callback(new Metric(time, value, _this)); });
};

// Computes a constant expression like the "7" in "x * 7"
function constant(db, callback) {
  var self = this, value = this.expression.value();
  walk(this.start, this.stop, this.tier, function(time){ callback(time, value); });
  this.emit('complete');
};

// Serializes a unary expression for computation.
function unary(db, callback) {
  var self = this,
      remaining = 0,
      time0 = Date.now(),
      name = this.expression.source,
      queue = queueByName[name];

  // Compute the expected number of values.
  walk(this.start, this.stop, this.tier, function(time){ ++remaining; });

  // If no results were requested, return immediately.
  if (!remaining) return callback(stop);

  // Add this task to the appropriate queue.
  if (queue) queue.next = task;
  else process.nextTick(task);
  queueByName[name] = task;

  function task() {
    findOrComputeUnary.call(self, db, function(time, value) {
      callback(time, value);
      if (!--remaining) {
        self.emit('complete');
        if (task.next) process.nextTick(task.next);
        else delete queueByName[name];

        // Record how long it took us to compute as an event!
        var time1 = Date.now();
        metalog.event("cube_compute", {
          expression: self.expression.source,
          ms: time1 - time0
        });
      }
    }, self);
  }
}

// Finds or computes a unary (primary) expression.
function findOrComputeUnary(db, callback) {
  var measurement = this,
      expression = this.expression,
      name   = expression.type,
      map    = expression.value,
      reduce = reduces[expression.reduce];

  find(measurement, callback);

  // The metric is computed recursively, reusing the above variables.
  function find(measurement, callback) {
    var start  = measurement.start,
        stop   = measurement.stop,
        tier   = measurement.tier,
        compute = ((tier.next && reduce.pyramidal) ? computePyramidal : computeFlat),
        time    = start;

    // Query for the desired metric in the cache.
    Metric.find(db, measurement, foundMetrics);

    // Immediately report back whatever we have. If any values are missing,
    // merge them into contiguous intervals and asynchronously compute them.
    function foundMetrics(error, metric) {
      handle(error);
      if (metric) {
        callback(metric.time, metric.value);                 // send back value for this timeslot
        if (time < metric.time) compute(time, metric.time);  // recurse from last value seen up to this timeslot
        time = tier.step(metric.time);                       // update the last-observed timeslot
      } else {
        if (time < stop) compute(time, stop);                // once last row is seen, compute rest of range
      }
    }

    // Group metrics from the next tier.
    function computePyramidal(start, stop) {
      var bins = {},
          measurement = new Measurement(expression, start, stop, tier.next);
      // metalog.warn('computePyramidal', { expr: expression.source, start: start, stop: stop, tier: tier.key, tr: tr });

      find(measurement, function(time, value) {
        var bin = bins[time = tier.floor(time)] || (bins[time] = {size: tier.size(time), values: []});
        if (bin.values.push(value) === bin.size) {
          callback(time, reduce(bin.values));
          delete bins[time];
        }
      });
    }

    // Group raw events. Unlike the pyramidal computation, here we can control
    // the order in which rows are returned from the database. Thus, we know
    // when we've seen all of the events for a given time interval.
    function computeFlat(start, stop) {
      // metalog.warn('computeFlat', { expr: expression.source, start: start, stop: stop, tier: tier.key });

      // if (tier.floor(start) < new Date(new Date() - options.horizons.calculation)){
      //   metalog.info('cube_compute', {is: 'past_horizon', metric: metric });
      //   start = tier.step(tier.floor(new Date(new Date() - options.horizons.calculation)))
      // }

      var measurement = new Measurement(expression, start, stop, tier);
      var time = start, values = [];


      Event.find(db, measurement, function(error, event) {
        handle(error);
        if (event) {
          var then = tier.floor(event.time);
          if (time < then) {
            callback(time, values.length ? reduce(values) : reduce.empty);
            while ((time = tier.step(time)) < then) callback(time, reduce.empty);
            values = [map(event.to_wire())];
          } else {
            values.push(map(event.to_wire()));
          }
        } else {
          callback(time, values.length ? reduce(values) : reduce.empty);
          while ((time = tier.step(time)) < stop) callback(time, reduce.empty);
        }
      });
    }
  }
}

// Computes a binary expression by merging two subexpressions
//
// "sum(req) - sum(resp)" will op ('-') the result of unary "sum(req)" and
// unary "sum(resp)". We don't know what order they'll show up in, so if say
// the value for left appears first, it parks that value as left[time], where
// the result for right will eventually find it.
function binary(db, callback) {
  var self = this, expression = this.expression, value;
  var left  = new Measurement(expression.left,  this.start, this.stop, this.tier),
      right = new Measurement(expression.right, this.start, this.stop, this.tier);

  left.on("complete", function(){
    var time = self.stop;
    if (time in right){
      self.emit("complete");
    } else {
      left[time] = undefined;
    }
  });

  right.on("complete", function(){
    var time = self.stop;
    if (time in left){
      self.emit("complete");
    } else {
      right[time] = undefined;
    }
  });

  left.measure(db, function(metric) {
    var time = metric.time, value = metric.value;
    if (time in right) {  // right val already appeared; get a result
      callback(time, expression.op(value, right[time]));
      delete right[time];
    } else {              // right val still on the way; stash the value
      left[time] = value;
    }
  });

  right.measure(db, function(metric) {
    var time = metric.time, value = metric.value;
    if (time in left) {
      callback(time, expression.op(left[time], value));
      delete left[time];
    } else {
      right[time] = value;
    }
  });
};

// execute cb on each interval from t1 to t2
function walk(t1, t2, tier, cb){
  while (t1 < t2) {
    cb(t1, t2);
    t1 = tier.step(t1);
  }
}

function handle(error) {
  if (!error) return;
  metalog.error('measurement', error);
  throw error;
}

module.exports = Measurement;
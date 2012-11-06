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
  this.start       = start;
  this.stop        = stop;
  this.tier        = tier;
  this.flavor      = (expression.op ? 'binary' : (expression.type ? 'unary' : 'constant'));
  this.isPyramidal = expression.type && reduces[expression.reduce].pyramidal;

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
Measurement.prototype.measure = function measure(db, options, callback) {
  var _this = this;
  compute[this.flavor].call(this, db, options, callback);
};

// Computes a constant expression like the "7" in "x * 7"
function constant(db, options, callback) {
  var _this = this, value = this.expression.value();
  walk(this.start, this.stop, this.tier, function(time){ callback(new Metric(time, value, _this)); });
  this.emit('complete');
};

// Serializes a unary expression for computation.
function unary(db, options, callback) {
  var self = this,
      remaining = 0,
      time0 = Date.now(),
      name = this.expression.source,
      queue = queueByName[name];

  // Compute the expected number of values.
  walk(this.start, this.stop, this.tier, function(time){ ++remaining; });

  // If no results were requested, return immediately.
  if (!remaining) return this.emit('complete');

  // Add this task to the appropriate queue.
  if (queue) queue.next = task;
  else process.nextTick(task);
  queueByName[name] = task;

  function task() {
    findOrComputeUnary.call(self, db, options, function(metric) {
      callback(metric);
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
    });
  }
}

// Finds or computes a unary (primary) expression.
function findOrComputeUnary(db, options, callback) {
  var expression  = this.expression,
      map         = expression.value,
      reduce      = reduces[expression.reduce],
      measurement = this;

  find(measurement, callback);

  // The metric is computed recursively, reusing the above variables.
  function find(measurement, callback) {
    var start  = measurement.start,
        stop   = measurement.stop,
        tier   = measurement.tier,
        compute = ((tier.next) ? computePyramidal : computeFlat),
        time    = start;

    // Query for the desired metric in the cache.
    Metric.find(db, measurement, foundMetrics);

    // Immediately report back whatever we have. If any values are missing,
    // merge them into contiguous intervals and asynchronously compute them.
    function foundMetrics(error, metric) {
      handle(error);
      if (metric) {
        callback(metric);                                    // send back value for this timeslot
        if (time < metric.time) compute(time, metric.time);  // recurse from last value seen up to this timeslot
        time = tier.step(metric.time);                       // update the last-observed timeslot
      } else {
        if (time < stop) compute(time, stop);                // once last row is seen, compute rest of range
      }
    }

    // Group metrics from the next tier.
    function computePyramidal(start, stop) {
      var bins = {},
          query_measurement = new Measurement(expression, start, stop, tier.next);

      find(query_measurement, function(metric) {
        var value = metric.value, time = metric.time, values = metric.values;
        var bin = bins[time = tier.floor(time)] || (bins[time] = {size: tier.size(time), values: []});

        if (reduce.pyramidal) bin.values.push(value);
        else bin.values = bin.values.concat(values||[]);

        if (!--bin.size) {
          var metric = new Metric(time, reduce(bin.values), measurement, bin.values);
          if (metric.value || metric.value === 0) metric.save(db, handle);
          callback(metric);
          delete bins[time];
        }
      });
    }

    // Group raw events. Unlike the pyramidal computation, here we can control
    // the order in which rows are returned from the database. Thus, we know
    // when we've seen all of the events for a given time interval.
    function computeFlat(start, stop) {
      // Reset start time to calculation horizon if requested time span goes past it
      if (options.horizons && tier.floor(start) < new Date(new Date() - options.horizons.calculation)){
        var old_start = start,
            start = tier.step(tier.floor(new Date(new Date() - options.horizons.calculation)))
        metalog.info('cube_compute', {is: 'past_horizon', start: {was: old_start, updated_to: start}, stop: stop, tier: tier, expression: expression.source });
      }

      // Reset stop time to calculation horizon if requested time span goes past it
      if (options.horizons && tier.floor(stop) < new Date(new Date() - options.horizons.calculation)){
        var old_stop = stop,
            stop = tier.step(tier.floor(new Date(new Date() - options.horizons.calculation)))
        metalog.info('cube_compute', {is: 'past_horizon', metric: {start: start, stop: {was: old_stop, updated_to: stop}, tier: tier, expression: expression.source } });
      }

      var query_measurement = new Measurement(expression, start, stop, tier);
      var time = start, values = [];

      function flat_callback(time, value, values){
        var metric = new Metric(time, value, measurement, values);
        callback(metric);
        if (metric.value || metric.value === 0) metric.save(db, handle);
      }

      Event.find(db, query_measurement, function(error, event) {
        handle(error);
        if (event) {
          var then = tier.floor(event.time);
          if (time < then) {
            flat_callback(time, (values.length ? reduce(values) : reduce.empty), values);
            while ((time = tier.step(time)) < then) flat_callback(time, reduce.empty);
            values = [map(event.to_wire())];
          } else {
            values.push(map(event.to_wire()));
          }
        } else {
          flat_callback(time, (values.length ? reduce(values) : reduce.empty), values);
          while ((time = tier.step(time)) < stop) flat_callback(time, reduce.empty);
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
function binary(db, options, callback) {
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

  left.measure(db, options, function(metric) {
    var time = metric.time, value = metric.value;
    if (time in right) {  // right val already appeared; get a result
      callback(new Metric(time, expression.op(value, right[time])));
      delete right[time];
    } else {              // right val still on the way; stash the value
      left[time] = value;
    }
  });

  right.measure(db, options, function(metric) {
    var time = metric.time, value = metric.value;
    if (time in left) {
      callback(new Metric(time, expression.op(left[time], value)));
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
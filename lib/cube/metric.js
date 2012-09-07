'use strict';

// TODO use expression ids or hashes for more compact storage

var parser  = require("./metric-expression"),
    tiers   = require("./tiers"),
    db      = require("./db"),
    mongodb = require("mongodb"),
    reduces = require("./reduces"),
    event   = require("./event"),
    metalog = require('./metalog'),
    options = require('../../config/cube');

var metric_fields = {v: 1},
    metric_options = {sort: {"_id.t": 1}, batchSize: 1000},
    event_options = {sort: {t: 1}, batchSize: 1000};

// Query for metrics.
exports.getter = function(db) {
  var Double       = mongodb.Double,
      queueByName  = {},
      async_queues = {},
      qcount       = 0;

  function getter(request, callback) {
    var measurement, expression,
        tier  = tiers[+request.step],
        start = new Date(request.start),
        stop  = new Date(request.stop);
    try {
      if (!tier)        throw "invalid step";
      if (isNaN(start)) throw "invalid start";
      if (isNaN(stop))  throw "invalid stop";
      // if (request.expression && (request.expression.match(/\(/mg).length > 2)){ throw("rejected complex expression"); }

      // Round start and stop to the appropriate time step.
      start       = tier.floor(start);
      stop        = tier.ceil(stop);
      expression  = parser.parse(request.expression);
      measurement = new Measurement(expression, start, stop, tier);
    } catch(err) { return callback({error: err}), -1; }

    // Compute the request metric!
    measurement.measure(
      (("id" in request) ?
            function(time, value){ callback({time: time, value: value, id: request.id}); } :
            function(time, value){ callback({time: time, value: value}); }));
  }

  function Measurement(expression, start, stop, tier){
    // Round the start/stop to the tier edges
    this.start      = start;
    this.stop       = stop;
    this.tier       = tier;
    this.expression = expression;
  }

  Measurement.prototype.flavor  = function(){ return this.expression.op ? 'binary' : (this.expression.type ? 'unary' : 'constant'); };
  Measurement.prototype.inspect = function(){
    return { flavor: this.flavor(), tier: this.tier.key, start: this.tier.bin(this.start), stop: this.tier.bin(this.stop), expr: this.expression.source };
  };

  // Computes the metric for the given expression for the time interval from
  // start (inclusive) to stop (exclusive). The time granularity is determined
  // by the specified tier, such as daily or hourly. The callback is invoked
  // repeatedly for each metric value, being passed two arguments: the time and
  // the value. The values may be out of order due to partial cache hits.
  Measurement.prototype.measure = function measure(callback) {
    switch(this.flavor()){
      case 'binary':   this.binary(callback);   break;
      case 'unary':    this.unary(callback);    break;
      case 'constant': this.constant(callback); break;
    }
  };

  // Computes a constant expression like the "7" in "x * 7"
  Measurement.prototype.constant = function constant(callback) {
    var value = this.expression.value();
    walk(this.start, this.stop, this.tier, function(time){ callback(time, value); });
    callback(this.stop);
  };

  // Serializes a unary expression for computation.
  Measurement.prototype.unary = function unary(callback) {
    var measurement = this, expression = this.expression, start = this.start, stop = this.stop, tier = this.tier;
    var remaining = 0,
        time0 = Date.now(),
        time = start,
        name = expression.source,
        queue = queueByName[name],
        step = tier.key;
    metalog.minor('unary start', measurement.inspect(), start);

    // Compute the expected number of values.
    walk(start, stop, tier, function(time){ remaining++; });
    // If no results were requested, return immediately.
    if (!remaining) return callback(stop);

    // Add this task to the appropriate queue.
    task.qcount = qcount++;
    if (queue) queue.next = task;
    else process.nextTick(task);
    queueByName[name] = task;

    function task() {
      findOrComputeUnary(expression, start, stop, tier, function(time, value){
        callback(time, value);
        metalog.minor('unary result', { meas: measurement.inspect(), qcount: qcount, next: (task.next && task.next.qcount) });
        if (!--remaining) {
          callback(stop);
          if (task.next) process.nextTick(task.next);
          else delete queueByName[name];
          // Record how long it took us to compute as an event!
          metalog.minor("cube_compute", { is: 'metric', at: 'done', meas: measurement.inspect(), ms: Date.now() - time0});
        }
      });
    }
    return null;
  };

  // Finds or computes a unary (primary) expression.
  function findOrComputeUnary(expression, start, stop, tier, callback) {
    var name   = expression.type,
        map    = expression.value,
        reduce = reduces[expression.reduce],
        filter = {t: {}},
        fields = {t: 1},
        metrics, events;

    metalog.minor('findOrComputeUnary', expression.source, start, stop, tier.key, callback.toString());

    // if (!reduce) return callback({error: "invalid reduce operation"}), -1;

    // Copy any expression filters into the query object.
    expression.filter(filter);

    // Request any needed fields.
    expression.fields(fields);

    db.metrics(name, function(error, collection){
      handle(error);

      metrics = collection;
      find(start, stop, tier, callback);
    });

    // The metric is computed recursively, reusing the above variables.
    function find(start, stop, tier, callback) {
      var compute = ((tier.next && reduce.pyramidal) ? computePyramidal : computeFlat),
          step    = tier.key;

      // Query for the desired metric in the cache.
      metrics.find({
        i: false,
        "_id.e": expression.source,
        "_id.l": tier.key,
        "_id.t": {
          $gte: start,
          $lt: stop
        }
      }, metric_fields, metric_options, foundMetrics);

      // Immediately report back whatever we have. If any values are missing,
      // merge them into contiguous intervals and asynchronously compute them.
      function foundMetrics(error, cursor) {
        metalog.minor('foundMetrics', expression.source, start, stop, tier.key, callback.toString());
        handle(error);
        var time = start;
        cursor.each(function(error, row) {

          handle(error);
          if (row) {
            callback(row._id.t, row.v);
            if (time < row._id.t) compute(time, row._id.t);
            time = tier.step(row._id.t);
          } else {
            if (time < stop) compute(time, stop);
          }
        });
      }

      // Group metrics from the next tier.
      function computePyramidal(start, stop) {
        metalog.minor('computePyramidal', expression.source, start, stop, tier.key, callback.toString());
        var bins = {};
        find(start, stop, tier.next, function(time, value) {
          var bin = bins[time = tier.floor(time)] || (bins[time] = {size: tier.size(time), values: []});
          if (bin.values.push(value) === bin.size) {
            save(time, reduce(bin.values));
            delete bins[time];
          }
        });
      }

      function get_mbox(meas){
        return [meas.tier.key, meas.tier.bin(meas.start), meas.tier.bin(meas.stop), meas.expression.source, meas.fields].join('~');
      }

      // Group raw events. Unlike the pyramidal computation, here we can control
      // the order in which rows are returned from the database. Thus, we know
      // when we've seen all of the events for a given time interval.
      function computeFlat(start, stop) {
        // if (tier.floor(start) < new Date(new Date() - options.horizons.calculation)){
        //   metalog.info('cube_compute', {is: 'past_horizon', metric: metric });
        //   start = tier.step(tier.floor(new Date(new Date() - options.horizons.calculation)))
        // }
        filter.t.$gte = start;
        filter.t.$lt = stop;

        db.events(name, function _computeFlatOnComplete(error, collection) {
          handle(error);

          collection.find(filter, fields, event_options, function(error, cursor) {
            handle(error);
            var time = start, values = [];
            cursor.each(function(error, row) {
              handle(error);
              if (row) {
                var then = tier.floor(row.t);
                if (time < then) {
                  save(time, values.length ? reduce(values) : reduce.empty);
                  while ((time = tier.step(time)) < then) save(time, reduce.empty);
                  values = [map(row)];
                } else {
                  values.push(map(row));
                }
              } else {
                save(time, values.length ? reduce(values) : reduce.empty);
                while ((time = tier.step(time)) < stop) save(time, reduce.empty);
              }
            });
          });
        })
      }

      function save(time, value) {
        callback(time, value);
        if ((! value) && (value !== 0)) return;
        var metric = {
            _id: {
              e: expression.source,
              l: tier.key,
              t: time
            },
            i: false,
            v: new Double(value)
        };
        metalog.info('cube_compute', {is: 'metric_save', metric: metric });
        metrics.save(metric, handle);
      }
    }
  }

  // Computes a binary expression by merging two subexpressions
  //
  // "sum(req) - sum(resp)" will op ('-') the result of unary "sum(req)" and
  // unary "sum(resp)". We don't know what order they'll show up in, so if say
  // the value for left appears first, it parks that value as left[time], where
  // the result for right will eventually find it.
  Measurement.prototype.binary = function binary(callback) {
    var expression = this.expression, start = this.start, stop = this.stop, tier = this.tier;
    var left  = new Measurement(expression.left,  start, stop, tier),
        right = new Measurement(expression.right, start, stop, tier);

    left.measure(function(time, vall) {
      if (time in right) {  // right val already appeared; get a result
        callback(time, time < stop ? expression.op(vall, right[time]) : vall);
        delete right[time];
      } else {              // right val still on the way; stash the value
        left[time] = vall;
      }
    });

    right.measure(function(time, valr) {
      if (time in left) {
        callback(time, time < stop ? expression.op(left[time], valr) : valr);
        delete left[time];
      } else {
        right[time] = valr;
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

  return getter;
};

function handle(error) {
  if (!error) return;
  metalog.warn('cube_request', {is: 'metric error', error: error });
  throw error;
}

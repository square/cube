'use strict';

// TODO use expression ids or hashes for more compact storage

var _ = require("underscore"),
    util    = require("util"),
    queuer  = require("../queue-async/queue"),
    parser  = require("./metric-expression"),
    tiers   = require("./tiers"),
    reduces = require("./reduces"),
    models  = require("./models"), Metric = models.Metric,
    event   = require("./event"),
    metalog = require('./metalog'),
    options = require('../../config/cube');

var metric_fields = {v: 1},
    metric_options = {sort: {"_id.t": 1}, batchSize: 1000},
    event_options = {sort: {t: 1}, batchSize: 1000},
    queue_parallelism = 1;

// Query for metrics.
exports.getter = function(db){
  var Double       = require("mongodb").Double,
      queueByName  = {},
      request_queues = {},
      qcount         = 0;

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
      measurement = new Measurement(expression, start, stop, tier, send_response);
    } catch(error) {
      metalog.error('mget', error, { info: util.inspect([start, stop, tier, expression] )});
      return callback({error: error, _trace: request._trace}), -1;
    }

    function send_response(time, value, tr){
      var resp = new Metric(time, value, request.id, measurement);
      metalog.dump_trace('resp', resp, {using: tr, bin: resp.bin });
      callback(resp);
    }

    // Compute the request metric!
    metalog.trace('mget', measurement, { using: request });
    measurement.measure();
  }

  function Measurement(expression, start, stop, tier, sender){
    // Round the start/stop to the tier edges
    this.expression = expression;
    this.start      = start;
    this.stop       = stop;
    this.tier       = tier;
    this.sender     = sender;
    this.flavor     = (expression.op ? 'binary' : (expression.type ? 'unary' : 'constant'));

    // Object.defineProperties(this, {
    // });
  }
  Measurement.prototype.report = function(){
    return { flavor: this.flavor, tier: this.tier.key, start: this.tier.bin(this.start), stop: this.tier.bin(this.stop), expr: (this.expression.op||this.expression.source||this.expression.value()) };
  };

  Measurement.prototype.send_result = function(time, value, tr){
    var ret_tr = metalog.dump_trace('mres', tr, { using: this, bin: this.tier.bin(time), val: value });
    this.sender(time, value, ret_tr);
  };
  Measurement.prototype.complete = function(tr){ this.send_result(this.stop, null, tr); };

  // Computes the metric for the given expression for the time interval from
  // start (inclusive) to stop (exclusive). The time granularity is determined
  // by the specified tier, such as daily or hourly. The callback is invoked
  // repeatedly for each metric value, being passed two arguments: the time and
  // the value. The values may be out of order due to partial cache hits.
  Measurement.prototype.measure = function measure() {
    metalog.trace('meas', this, this.report());
    this[this.flavor](this.sender);
  };

  // Computes a constant expression like the "7" in "x * 7"
  Measurement.prototype.constant = function constant() {
    var self = this, value = this.expression.value();
    walk(this.start, this.stop, this.tier, function(time){ self.send_result(time, value); });
    self.complete();
  };

  // // Serializes a unary expression for computation.
  // Measurement.prototype.unary = function unary() {
  //   var self      = this,
  //       remaining = 0,
  //       queue     = get_queue(this.expression.source);
  // 
  //   // Compute the expected number of values; if no results were requested, return immediately.
  //   walk(self.start, self.stop, self.tier, function(time){ remaining++; });
  //   if (! remaining) return this.complete();
  // 
  //   // Add this task to the appropriate queue.
  //   queue.defer(function task(q_callback){
  //     self.run_unary(function(time, value, tr){
  //       self.send_result(time, value);
  //       if (!--remaining) {
  //         process.nextTick(function(){ q_callback(null, [time, value]); });
  //         self.complete();
  //       }
  //     });
  //   }).await(_.identity);
  // }

  // Serializes a unary expression for computation.
  Measurement.prototype.unary = function unary(callback) {
    var self = this, expression = this.expression, start = this.start, stop = this.stop, tier = this.tier;
    var remaining = 0,
    time0 = Date.now(),
    name = expression.source,
    queue = queueByName[name],
    step = tier.key;

    // Compute the expected number of values.
    walk(start, stop, tier, function(time){ ++remaining; });

    // If no results were requested, return immediately.
    if (!remaining) return callback(stop);

    // Add this task to the appropriate queue.
    if (queue) queue.next = task;
    else process.nextTick(task);
    queueByName[name] = task;

    function task() {
      findOrComputeUnary(expression, start, stop, tier, function(time, value, tr) {
        self.send_result(time, value, tr);
        if (!--remaining) {
          self.complete(tr);
          if (task.next) process.nextTick(task.next);
          else delete queueByName[name];

          // Record how long it took us to compute as an event!
          var time1 = Date.now();
          metalog.event("cube_compute", {
            expression: expression.source,
            ms: time1 - time0
          });
        }
      }, self);
    }
  }

  // Finds or computes a unary (primary) expression.
  function findOrComputeUnary(expression, start, stop, tier, callback, tr) {
    var name   = expression.type,
    map    = expression.value,
    reduce = reduces[expression.reduce],
    filter = {t: {}},
    fields = {t: 1},
    metrics, events;

    metalog.trace('find', this);

    // Copy any expression filters into the query object.
    expression.filter(filter);
    // Request any needed fields.
    expression.fields(fields);

    db.metrics(name, function(error, collection){
      handle(error);
      metrics = collection;
      find(start, stop, tier, tr, callback);
    });

    // The metric is computed recursively, reusing the above variables.
    function find(start, stop, tier, tr, callback) {
      var compute = ((tier.next && reduce.pyramidal) ? computePyramidal : computeFlat),
      step    = tier.key;

      metalog.trace('mf0', tr);

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
        handle(error);
        var time = start;
        cursor.each(function(error, row) {
          var mftr = metalog.trace('mf1', {}, { using: tr });

          handle(error);
          if (row) {
            callback(row._id.t, row.v, metalog.trace('mfy', mftr));   // send back value for this timeslot
            if (time < row._id.t) compute(time, row._id.t, mftr);     // recurse from last value seen up to this timeslot
            time = tier.step(row._id.t);                              // update the last-observed timeslot
          } else {
            if (time < stop) compute(time, stop, mftr);               // once last row is seen, compute rest of range
          }
        });
      }

      // Group metrics from the next tier.
      function computePyramidal(start, stop, tr) {
        var bins = {};
        //
        // metalog.warn('computePyramidal', { expr: expression.source, start: start, stop: stop, tier: tier.key, tr: tr });
        metalog.trace('mfl0', tr, { start: start, stop: stop });
        find(start, stop, tier.next, tr, function(time, value, tr) {
          var bin = bins[time = tier.floor(time)] || (bins[time] = {size: tier.size(time), values: []});
          if (bin.values.push(value) === bin.size) {
            save(time, reduce(bin.values));
            delete bins[time];
          }
        });
      }

      // Group raw events. Unlike the pyramidal computation, here we can control
      // the order in which rows are returned from the database. Thus, we know
      // when we've seen all of the events for a given time interval.
      function computeFlat(start, stop, tr) {
        // metalog.warn('computeFlat', { expr: expression.source, start: start, stop: stop, tier: tier.key });
        metalog.trace('mfl0', tr, { start: start, stop: stop });
        
        // if (tier.floor(start) < new Date(new Date() - options.horizons.calculation)){
        //   metalog.info('cube_compute', {is: 'past_horizon', metric: metric });
        //   start = tier.step(tier.floor(new Date(new Date() - options.horizons.calculation)))
        // }
        filter.t.$gte = start;
        filter.t.$lt = stop;

        db.events(name, function (error, collection) {
          handle(error);

          collection.find(filter, fields, event_options, function(error, cursor) {
            handle(error);
            var time = start, values = [];
            cursor.each(function(error, row) {
              var res_tr = metalog.trace('mflt', {}, { using: tr });
              handle(error);
              if (row) {
                var then = tier.floor(row.t);
                if (time < then) {
                  save(time, values.length ? reduce(values) : reduce.empty, res_tr);
                  while ((time = tier.step(time)) < then) save(time, reduce.empty, res_tr);
                  values = [map(row)];
                } else {
                  values.push(map(row));
                }
              } else {
                save(time, values.length ? reduce(values) : reduce.empty, res_tr);
                while ((time = tier.step(time)) < stop) save(time, reduce.empty, res_tr);
              }
            });
          });
        })
      }

      function save(time, value, tr) {
        callback(time, value, metalog.trace('msav', tr));
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
        // metalog.trace('cube_compute', {is: 'metric_save', metric: metric });
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
  Measurement.prototype.binary = function binary() {
    var self = this, expression = this.expression, value;
    var left  = new Measurement(expression.left,  this.start, this.stop, this.tier, this.id),
        right = new Measurement(expression.right, this.start, this.stop, this.tier, this.id);
    metalog.trace('msb0', left,  {using: self}); metalog.trace('msb0', right,  {using: self}); 

    left.sender = function(time, vall, tr) {
      if (time in right) {  // right val already appeared; get a result
        self.send_result(time, (time < self.stop ? expression.op(vall, right[time]) : vall), metalog.trace('msb', tr));
        delete right[time];
      } else {              // right val still on the way; stash the value
        left[time] = vall;
      }
    };

    right.sender = function(time, valr, tr) {
      if (time in left) {
        self.send_result(time, (time < self.stop ? expression.op(left[time], valr) : valr), metalog.trace('msb', tr));
        delete left[time];
      } else {
        right[time] = valr;
      }
    };
    
    left.measure();
    right.measure();
  };

  function get_queue(name){
    if (! request_queues[name]) request_queues[name] = queuer(queue_parallelism);
    return request_queues[name];
  }

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
  metalog.error('metric', error);
  throw error;
}

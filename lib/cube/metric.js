'use strict';

// TODO use expression ids or hashes for more compact storage

var _ = require("underscore"),
    mongodb = require("mongodb"),
    util    = require("util"),
    queuer  = require("../queue-async/queue"),
    db      = require("./db"),
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
    metric_parallelism = 5;

// Query for metrics.
exports.getter = function(db){
  var Double       = mongodb.Double,
      queueByName  = {},
      request_queues = {},
      qcount         = 0;

  function getter(request, callback) {
    var measurement, expression,
        tier  = tiers[+request.step],
        start = new Date(request.start),
        stop  = new Date(request.stop);
    metalog.trace('m_get', request);
    
    try {
      if (!tier)        throw "invalid step";
      if (isNaN(start)) throw "invalid start";
      if (isNaN(stop))  throw "invalid stop";
      // if (request.expression && (request.expression.match(/\(/mg).length > 2)){ throw("rejected complex expression"); }

      // Round start and stop to the appropriate time step.
      start       = tier.floor(start);
      stop        = tier.ceil(stop);
      expression  = parser.parse(request.expression);
      measurement = new Measurement(expression, start, stop, tier, request.id, callback);
    } catch(error) {
      metalog.error('m_get', error, { info: util.inspect([start, stop, tier, expression] )});
      return callback({error: error, _trace: request._trace}), -1;
    }

    // Compute the request metric!
    metalog.trace('m_run', measurement, _.extend({ using: request }, measurement.report()));
    measurement.measure();
  }

  function Measurement(expression, start, stop, tier, id, sender){
    // Round the start/stop to the tier edges
    this.expression = expression;
    this.start      = start;
    this.stop       = stop;
    this.tier       = tier;
    this.id         = id;
    this.sender     = sender;
  }

  Measurement.prototype.flavor  = function(){ return this.expression.op ? 'binary' : (this.expression.type ? 'unary' : 'constant'); };
  Measurement.prototype.report = function(){
    return { flavor: this.flavor(), tier: this.tier.key, start: this.tier.bin(this.start), stop: this.tier.bin(this.stop), expr: this.expression.source };
  };

  Measurement.prototype.send_result = function(time, value){
    var resp = new Metric(time, value, this.id, this);
    metalog.dump_trace('m_res', resp, {using: this, bin: resp.bin });
    this.sender(resp);
  };
  Measurement.prototype.complete = function(){ this.send_result(this.stop, null); };


  // Computes the metric for the given expression for the time interval from
  // start (inclusive) to stop (exclusive). The time granularity is determined
  // by the specified tier, such as daily or hourly. The callback is invoked
  // repeatedly for each metric value, being passed two arguments: the time and
  // the value. The values may be out of order due to partial cache hits.
  Measurement.prototype.measure = function measure() {
    switch(this.flavor()){
      case 'binary':   this.binary(this.sender);   break;
      case 'unary':    this.unary(this.sender);    break;
      case 'constant': this.constant(); break;
    }
  };

  // Computes a constant expression like the "7" in "x * 7"
  Measurement.prototype.constant = function constant() {
    var self = this, value = this.expression.value();
    walk(this.start, this.stop, this.tier, function(time){ self.send_result(time, value); });
    self.complete();
  };

  // Computes a binary expression by merging two subexpressions
  //
  // "sum(req) - sum(resp)" will op ('-') the result of unary "sum(req)" and
  // unary "sum(resp)". We don't know what order they'll show up in, so if say
  // the value for left appears first, it parks that value as left[time], where
  // the result for right will eventually find it.
  Measurement.prototype.binary = function binary(callback) {
    var self = this, expression = this.expression, start = this.start, stop = this.stop, tier = this.tier, value;
    var left  = new Measurement(expression.left,  start, stop, tier),
        right = new Measurement(expression.right, start, stop, tier);
    metalog.trace('msbl', left,  {using: self}); metalog.trace('msbr', right, {using: self});
    
    left.measure(function(time, vall, tr) {
      if (time in right) {  // right val already appeared; get a result
        value = (time < stop ? expression.op(vall, right[time]) : vall);
        this.send_result(time, value);
        delete right[time];
      } else {              // right val still on the way; stash the value
        left[time] = vall;
      }
    });

    right.measure(function(time, valr, tr) {
      if (time in left) {
        value = (time < stop ? expression.op(left[time], valr) : valr)
        this.send_result(time, value);
        delete left[time];
      } else {
        right[time] = valr;
      }
    });
  };

  Measurement.prototype.unary = unary_new

  function get_queue(name){
    if (! request_queues[name]) request_queues[name] = queuer(metric_parallelism);
    return request_queues[name];
  }

  // Serializes a unary expression for computation.
  function unary_new(callback) {
    var self      = this,
        remaining = 0,
        queue     = get_queue(this.expression.source);

    // Compute the expected number of values; if no results were requested, return immediately.
    walk(self.start, self.stop, self.tier, function(time){ remaining++; });
    if (! remaining) return this.complete();

    // Add this task to the appropriate queue.
    queue.defer(function task(q_callback){
      self.findOrComputeUnary(function(time, value, tr){
        self.send_result(time, value);
        if (!--remaining) {
          process.nextTick(function(){ q_callback(null, [time, value]); });
          self.complete();
        }
      });
    }).await(_.identity);
  }

  // // Serializes a unary expression for computation.
  // function unary_old(callback) {
  //   var self = this, expression = this.expression, start = this.start, stop = this.stop, tier = this.tier;
  //   var remaining = 0,
  //       time0 = Date.now(),
  //       time = start,
  //       name = expression.source,
  //       queue = queueByName[name],
  //       step = tier.key;
  // 
  //   // Compute the expected number of values.
  //   walk(start, stop, tier, function(time){ remaining++; });
  //   // If no results were requested, return immediately.
  //   if (!remaining) return callback(stop, null, metalog.trace('msu', {}, { using: self}));
  // 
  //   // Add this task to the appropriate queue.
  //   task.qcount = qcount++;
  //   if (queue){ queue.next = task; queue.next.qindex = queue.qindex + 1; }
  //   else      { task.qindex = 0 ;  process.nextTick(task); }
  //   queueByName[name] = task;
  // 
  //   function task() {
  //     findOrComputeUnary(expression, start, stop, tier, function(time, value){
  //       callback(time, value);
  //       // metalog.warn('unary result', { time: time, meas: self.report(), qcount: qcount, next: (task.next && task.next.qcount) });
  //       if (!--remaining) {
  //         callback(stop);
  //         if (task.next) process.nextTick(task.next);
  //         else delete queueByName[name];
  //         // Record how long it took us to compute as an event!
  //         // metalog.warn("cube_compute", { is: 'metric', at: 'done', meas: self.report(), ms: Date.now() - time0});
  //       }
  //     });
  //   }
  //   return null;
  // }

  // Finds or computes a unary (primary) expression.
  Measurement.prototype.findOrComputeUnary = function findOrComputeUnary(callback) {
    var self = this, expression = this.expression, start = this.start, stop = this.stop, tier = this.tier;
    var name   = expression.type,
        map    = expression.value,
        reduce = reduces[expression.reduce],
        filter = {t: {}},
        fields = {t: 1},
        metrics, events;

    metalog.trace('focu', this);

    // Copy any expression filters into the query object.
    expression.filter(filter);
    // Request any needed fields.
    expression.fields(fields);

    db.metrics(name, function(error, collection){
      handle(error);
      metrics = collection;
      find(start, stop, tier, self, callback);
    });

    // The metric is computed recursively, reusing the above variables.
    function find(start, stop, tier, tr, callback) {
      var compute = ((tier.next && reduce.pyramidal) ? computePyramidal : computeFlat),
          step    = tier.key;

      metalog.trace('m_f0', tr);
      
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
          var fmtr = metalog.trace('m_f1', {}, { using: tr });

          handle(error);
          if (row) {
            callback(row._id.t, row.v, fmtr);                      // send back value for this timeslot
            if (time < row._id.t) compute(time, row._id.t, fmtr);  // recurse from last value seen up to this timeslot
            time = tier.step(row._id.t);                           // update the last-observed timeslot
          } else {
            if (time < stop) compute(time, stop, fmtr);            // once last row is seen, compute rest of range
          }
        });
      }

      // Group metrics from the next tier.
      function computePyramidal(start, stop, tr) {
        // metalog.warn('computePyramidal', { expr: expression.source, start: start, stop: stop, tier: tier.key, tr: tr });
        var bins = {};
        find(start, stop, tier.next, tr, function(time, value, tr) {
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
      function computeFlat(start, stop, tr) {
        metalog.trace('m_cf', tr, { expr: expression.source, start: start, stop: stop, tier: tier.key });
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
              var res_tr = metalog.trace('m_cfn', {}, { using: tr });
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
        callback(time, value, metalog.trace('m_sv', {}, {using: tr}));
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

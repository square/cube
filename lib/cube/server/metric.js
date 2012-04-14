var util = require("util"),
    parser = require("./metric-expression"),
    tiers = require("./tiers"),
    types = require("./types"),
    reduces = require("./reduces"),
    info = require("./info");

var metric_fields = {v: 1},
    metric_options = {sort: {"_id.t": 1}, batchSize: 1000},
    event_options = {sort: {t: 1}, batchSize: 1000};

// Query for metrics.
// TODO use expression ids, and record how often expressions are queried?
exports.getter = function(db) {
  var collection = types(db),
      Double = db.bson_serializer.Double,
      queueByName = {};

  // Computes the metric for the given expression for the time interval from
  // start (inclusive) to stop (exclusive). The time granularity is determined
  // by the specified tier, such as daily or hourly. The callback is invoked
  // repeatedly for each metric value, being passed two arguments: the time and
  // the value. The values may be out of order due to partial cache hits.
  function measure(expression, start, stop, tier, callback) {
    (expression.op ? binary : expression.type ? unary : constant)(expression, start, stop, tier, callback);
  }

  // Computes a constant expression;
  function constant(expression, start, stop, tier, callback) {
    var value = expression.value();
    while (start < stop) {
      callback(start, value);
      start = tier.step(start);
    }
    callback(stop);
  }

  // Serializes a unary expression for computation.
  function unary(expression, start, stop, tier, callback) {
    var remaining = 0,
        time = start,
        name = expression.type,
        queue = queueByName[name],
        step = tier.key;

    // Add this task to the appropriate queue.
    if (queue) queue.next = task;
    else process.nextTick(task);
    queueByName[name] = task;

    // Compute the expected number of values.
    while (time < stop) ++remaining, time = tier.step(time);
    info.stat("metrics_requested", name, step).add(remaining);

    function task() {
      findOrComputeUnary(expression, start, stop, tier, function(time, value) {
        callback(time, value);
        if (!--remaining) {
          callback(stop);
          if (task.next) process.nextTick(task.next);
          else delete queueByName[name];
        }
      });
    }
  }

  // Finds or computes a unary (primary) expression.
  function findOrComputeUnary(expression, start, stop, tier, callback) {
    var name = expression.type,
        type = collection(name),
        map = expression.value,
        reduce = reduces[expression.reduce],
        filter = {t: {}},
        fields = {t: 1};

    if (!reduce) return util.log("no such reduce: " + expression.reduce);

    // Copy any expression filters into the query object.
    expression.filter(filter);

    // Request any needed fields.
    expression.fields(fields);

    find(start, stop, tier, callback);

    // The metric is computed recursively, reusing the above variables.
    function find(start, stop, tier, callback) {
      var compute = tier.next && reduce.pyramidal ? computePyramidal : computeFlat,
          step = tier.key;

      // Query for the desired metric in the cache.
      type.metrics.find({
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
        if (error) throw error;
        var time = start;
        cursor.each(function(error, row) {
          if (error) throw error;
          if (row) {
            info.stat("metrics_cached", name, step).add(1);
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
        var bins = {};
        find(start, stop, tier.next, function(time, value) {
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
      function computeFlat(start, stop) {
        filter.t.$gte = start;
        filter.t.$lt = stop;
        type.events.find(filter, fields, event_options, function(error, cursor) {
          if (error) throw error;
          var time = start, values = [];
          cursor.each(function(error, row) {
            if (error) throw error;
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
      }

      function save(time, value) {
        callback(time, value);
        if (value) {
          type.metrics.save({
            _id: {
              e: expression.source,
              l: tier.key,
              t: time
            },
            i: false,
            v: new Double(value)
          }, function(error) {
            if (error) util.log("error: " + error);
          });
          info.stat("metrics_computed", name, step).add(1);
        } else {
          info.stat("metrics_zeroed", name, step).add(1);
        }
      }
    }
  }

  // Computes a binary expression by merging two subexpressions.
  function binary(expression, start, stop, tier, callback) {
    var left = {}, right = {};

    measure(expression.left, start, stop, tier, function(t, l) {
      if (t in right) {
        callback(t, t < stop ? expression.op(l, right[t]) : l);
        delete right[t];
      } else {
        left[t] = l;
      }
    });

    measure(expression.right, start, stop, tier, function(t, r) {
      if (t in left) {
        callback(t, t < stop ? expression.op(left[t], r) : r);
        delete left[t];
      } else {
        right[t] = r;
      }
    });
  }

  return function(request, callback) {
    var start = new Date(request.start),
        stop = new Date(request.stop),
        id = request.id;

    // Validate the dates.
    if (isNaN(start)) return util.log("invalid start: " + request.start), -1;
    if (isNaN(stop)) return util.log("invalid stop: " + request.stop), -1;

    // Parse the expression.
    // TODO store expression as a hash for smaller representation?
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (e) {
      return util.log("invalid expression: " + e), -1;
    }

    // Round start and stop to the appropriate time step.
    var tier = tiers[request.step];
    if (!tier) return util.log("invalid step: " + request.step), -1;
    start = tier.floor(start);
    stop = tier.ceil(stop);

    // Compute the request metric!
    measure(expression, start, stop, tier, "id" in request
        ? function(time, value) { callback({time: time, value: value, id: id}); }
        : function(time, value) { callback({time: time, value: value}); });
  };
};

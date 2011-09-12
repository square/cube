var util = require("util"),
    parser = require("./metric-expression"),
    tiers = require("./tiers"),
    types = require("./types"),
    reduces = require("./reduces");

var metric_fields = {t: 1, g: 1, v: 1},
    metric_options = {sort: {t: 1}, batchSize: 1000},
    event_options = {sort: {t: 1}, batchSize: 1000},
    update_options = {upsert: true};

// Query for metrics.
// TODO use expression ids, and record how often expressions are queried?
exports.getter = function(db) {
  var collection = types(db),
      Double = db.bson_serializer.Double;

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
  }

  // Computes a unary (primary) expression.
  function unary(expression, start, stop, tier, callback) {
    var type = collection(expression.type),
        map = expression.value,
        group = expression.group,
        reduce = reduces[expression.reduce],
        filter = {t: {}},
        fields = {t: 1};

    if (!reduce) return util.log("no such reduce: " + expression.reduce);

    // Prepare for grouping.
    if (group) {
      delete fields.t;
      var group_options = {sort: {}};
      group.fields(group_options.sort);
      group.fields(fields);
    }

    // Copy any expression filters into the query object.
    expression.filter(filter);

    // Request any needed fields.
    expression.fields(fields);

    find(start, stop, tier, callback);

    // The metric is computed recursively, reusing the above variables.
    function find(start, stop, tier, callback) {
      var compute = group ? (group.multi ? computeGroups : computeGroup)
          : tier.next && reduce.pyramidal ? computePyramidal
          : computeFlat;

      // Query for the desired metric in the cache.
      type.metrics.find({
        i: false,
        e: expression.source,
        l: tier.key,
        t: {
          $gte: start,
          $lt: stop
        }
      }, metric_fields, metric_options, foundMetrics);

      // Immediately report back whatever we have. If any values are missing,
      // merge them into contiguous intervals and asynchronously compute them.
      function foundMetrics(error, cursor) {
        if (error) return util.log(error);
        var time = start;
        cursor.each(function(error, row) {
          if (error) return util.log(error);
          if (row) {
            callback(row.t, row.v, row.g);
            if (time < row.t) compute(time, row.t);
            time = tier.step(row.t);
          } else {
            if (time < stop) compute(time, stop);
          }
        });
      }

      // Process each bin separately, sorting by group.
      function computeGroup(start, stop) {
        var next = tier.step(start);
        filter.t.$gte = start;
        filter.t.$lt = next;
        type.events.find(filter, fields, group_options, function(error, cursor) {
          if (error) return util.log(error);
          var k0, values;
          cursor.nextObject(function(error, row) {
            if (error) return util.log(error);
            if (!row) return;
            k0 = group.value(row);
            values = [map(row)];
            cursor.each(function(error, row) {
              if (error) return util.log(error);
              if (row) {
                var k1 = group.value(row);
                if (k0 != k1) {
                  saveGroup(start, values.length ? reduce(values) : reduce.empty, k0);
                  k0 = k1;
                  values = [map(row)];
                } else {
                  values.push(map(row));
                }
              } else {
                saveGroup(start, values.length ? reduce(values) : reduce.empty, k0);
                if (next < stop) computeGroup(next, stop);
              }
            });
          });
        });
      }

      // Process each bin separately, loading it entirely into memory.
      function computeGroups(start, stop) {
        var next = tier.step(start);
        filter.t.$gte = start;
        filter.t.$lt = next;
        type.events.find(filter, fields, event_options, function(error, cursor) {
          if (error) return util.log(error);
          var groups = {};
          cursor.each(function(error, row) {
            if (error) return util.log(error);

            if (!row) {
              for (var key in groups) saveGroup(start, reduce(groups[key]), key);
              return;
            }

            var keys = group.value(row), value = map(row);
            if (keys && keys.forEach) {
              var i = -1, n = keys.length;
              while (++i < n) storeGroup(keys[i], value);
            } else if (keys != null) {
              storeGroup(keys, value);
            }

            if (next < stop) computeGroup(next, stop);
          });

          function storeGroup(key, value) {
            var values = groups[key];
            if (values) values.push(value);
            else groups[key] = [value];
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
          if (error) return util.log(error);
          var time = start, values = [];
          cursor.each(function(error, row) {
            if (error) return util.log(error);
            if (row) {
              var then = tier.floor(row.t);
              if (time < then) {
                save(time, values.length ? reduce(values) : reduce.empty);
                while ((time = tier.step(time)) < then) callback(time, reduce.empty);
                values = [map(row)];
              } else {
                values.push(map(row));
              }
            } else {
              save(time, values.length ? reduce(values) : reduce.empty);
              while ((time = tier.step(time)) < stop) callback(time, reduce.empty);
            }
          });
        });
      }

      function saveGroup(time, value, group) {
        callback(time, value, group);
        if (value) type.metrics.update({
          e: expression.source,
          l: tier.key,
          t: time,
          g: group
        }, {
          $set: {
            i: false,
            v: new Double(value)
          }
        }, update_options);
      }

      function save(time, value) {
        callback(time, value);
        if (value) type.metrics.update({
          e: expression.source,
          l: tier.key,
          t: time
        }, {
          $set: {
            i: false,
            v: new Double(value)
          }
        }, update_options);
      }
    }
  }

  // Computes a binary expression by merging two subexpressions.
  function binary(expression, start, stop, tier, callback) {
    var left = {}, right = {};

    measure(expression.left, start, stop, tier, function(t, l) {
      if (t in right) {
        callback(t, expression.op(l, right[t]));
        delete right[t];
      } else {
        left[t] = l;
      }
    });

    measure(expression.right, start, stop, tier, function(t, r) {
      if (t in left) {
        callback(t, expression.op(left[t], r));
        delete left[t];
      } else {
        right[t] = r;
      }
    });
  }

  return function(request, callback) {

    // Validate the dates.
    var start = new Date(request.start),
        stop = new Date(request.stop),
        id = request.id;
    if (isNaN(start)) return util.log("invalid start: " + request.start);
    if (isNaN(stop)) return util.log("invalid stop: " + request.stop);

    // Parse the expression.
    // TODO store expression as JSON object, or compute canonical form
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      return util.log("invalid expression: " + error);
    }

    // Round start and stop to the appropriate time step.
    var tier = tiers[request.step];
    if (!tier) return util.log("invalid step: " + request.step);
    start = tier.floor(start);
    stop = tier.ceil(stop);

    function callbackGroupId(time, value, group) {
      callback({time: time, group: group, value: value, id: id});
    }

    function callbackGroup(time, value, group) {
      callback({time: time, group: group, value: value});
    }

    function callbackValueId(time, value) {
      callback({time: time, value: value, id: id});
    }

    function callbackValue(time, value) {
      callback({time: time, value: value});
    }

    // Find or compute the desired metric.
    measure(expression, start, stop, tier, expression.group
        ? ("id" in request ? callbackGroupId : callbackGroup)
        : ("id" in request ? callbackValueId : callbackValue));
  };
};

// TODO report failures?
// TODO include the event._id (and define a JSON encoding for ObjectId?)
// TODO allow the event time to change when updating (fix invalidation)
// TODO fix race condition between cache invalidation and metric computation

var util = require("util"),
    mongodb = require("mongodb"),
    parser = require("./event-expression"),
    tiers = require("./tiers"),
    types = require("./types");

var type_re = /^[a-z][a-zA-Z0-9_]+$/,
    invalidate = {$set: {i: true}},
    multi = {multi: true},
    event_options = {sort: {t: -1}, batchSize: 1000};

exports.putter = function(db) {
  var collection = types(db),
      flushInterval,
      flushTypes = {},
      flushDelay = 5000;

  function endpoint(request) {
    var time = new Date(request.time),
        type = request.type;

    // Validate the date and type.
    if (!type_re.test(type)) return util.log("invalid type: " + request.type);
    if (isNaN(time)) return util.log("invalid time: " + request.time);

    // Create the event object, to control which fields are addressible.
    var event = {t: time, d: request.data};

    // If an id is specified, promote it to Mongo's primary key.
    if ("id" in request) event._id = request.id;

    // Save the event.
    collection(type).events.save(event);

    // Queue invalidation of metrics for this type.
    var times = flushTypes[type] || (flushTypes[type] = [time, time]);
    if (time < times[0]) times[0] = time;
    if (time > times[1]) times[1] = time;
  }

  // Invalidate cached metrics.
  endpoint.flush = function() {
    var types = [];
    for (var type in flushTypes) {
      var metrics = collection(type).metrics,
          times = flushTypes[type];
      types.push(type);
      for (var tier in tiers) {
        var floor = tiers[tier].floor;
        metrics.update({
          i: false,
          l: +tier,
          t: {
            $gte: floor(times[0]),
            $lte: floor(times[1])
          }
        }, invalidate, multi);
      }
    }
    if (types.length) util.log("flush " + types.join(", "));
    flushTypes = {};
  };

  flushInterval = setInterval(endpoint.flush, flushDelay);

  return endpoint;
};

exports.getter = function(db) {
  var collection = types(db);

  function getter(request, callback) {
    var start = new Date(request.start),
        stop = new Date(request.stop);

    // Validate the dates.
    if (isNaN(start)) return util.log("invalid start: " + request.start);
    if (isNaN(stop)) return util.log("invalid stop: " + request.stop);

    // Parse the expression.
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      return util.log("invalid expression: " + error);
    }

    // Copy any expression filters into the query object.
    var filter = {t: {$gte: start, $lt: stop}};
    expression.filter(filter);

    // Request any needed fields.
    var fields = {t: 1};
    expression.fields(fields);

    // Query for the desired events.
    collection(expression.type).events.find(filter, fields, event_options, function(error, cursor) {
      if (error) throw error;
      cursor.each(function(error, event) {
        if (callback.closed) return cursor.close();
        if (error) throw error;
        if (event) callback({
          time: event.t,
          data: event.d
        });
      });
    });
  }

  getter.close = function(callback) {
    callback.closed = true;
  };

  return getter;
};

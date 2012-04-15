// TODO include the event._id (and define a JSON encoding for ObjectId?)
// TODO allow the event time to change when updating (fix invalidation)
// TODO fix race condition between cache invalidation and metric computation

var util = require("util"),
    mongodb = require("mongodb"),
    parser = require("./event-expression"),
    tiers = require("./tiers"),
    types = require("./types"),
    bisect = require("./bisect");

var type_re = /^[a-z][a-zA-Z0-9_]+$/,
    invalidate = {$set: {i: true}},
    multi = {multi: true},
    event_options = {sort: {t: -1}, batchSize: 1000},
    metric_options = {capped: true, size: 1e7, autoIndexId: true};

exports.putter = function(db) {
  var collection = types(db),
      eventCollectionByType = {},
      eventsToSaveByType = {},
      timesToInvalidateByTierByType = {},
      flushInterval,
      flushDelay = 5000;

  function putter(request) {
    var time = new Date(request.time),
        type = request.type;

    // Validate the date and type.
    if (!type_re.test(type)) return util.log("invalid type: " + type);
    if (isNaN(time)) return util.log("invalid time: " + request.time);

    // If an id is specified, promote it to Mongo's primary key.
    var event = {t: time, d: request.data};
    if ("id" in request) event._id = request.id;

    // If this is a known event type, save immediately.
    if (type in eventCollectionByType) return save(type, event);

    // If someone is already creating the event collection for this new type,
    // then append this event to the queue for later save.
    if (type in eventsToSaveByType) return eventsToSaveByType[type].push(event);

    // Otherwise, it's up to us to see if the collection exists, verify the
    // associated indexes, create the corresponding metrics collection, and save
    // any events that have queued up in the interim!

    // First add the new event to the queue.
    eventsToSaveByType[type] = [event];

    // If the events collection exists, then we assume the metrics & indexes do
    // too. Otherwise, we must create the required collections and indexes. Note
    // that if you want to customize the size of the capped metrics collection,
    // or add custom indexes, you can still do all that by hand.
    db.collectionNames(type + "_events", function(error, names) {
      var events = collection(type).events;
      if (names.length) return flush();

      // Events are indexed by time.
      events.ensureIndex({"t": 1}, handle);

      // Create a capped collection for metrics. Two indexes are required:
      // one for computing, and the other for invalidating.
      db.createCollection(type + "_metrics", metric_options, function(error, metrics) {
        handle(error);
        metrics.ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}, handle);
        metrics.ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1}, handle);
        flush();
      });

      // Flush any pending events to the new collection.
      function flush() {
        eventCollectionByType[type] = events;
        eventsToSaveByType[type].forEach(function(event) { save(type, event); });
        delete eventsToSaveByType[type];
      }
    });
  }

  // Save the event of the specified type.
  function save(type, event) {
    eventCollectionByType[type].save(event, handle);
    queueInvalidation(type, event);
  }

  // Schedule deferred invalidation of metrics for this type.
  // For each type and tier, track the metric times to invalidate.
  // The times are kept in sorted order for bisection.
  function queueInvalidation(type, event) {
    var timesToInvalidateByTier = timesToInvalidateByTierByType[type],
        time = event.t;
    if (timesToInvalidateByTier) {
      for (var tier in tiers) {
        var tierTimes = timesToInvalidateByTier[tier],
            tierTime = tiers[tier].floor(time),
            i = bisect(tierTimes, tierTime);
        if (tierTimes[i] > tierTime) tierTimes.splice(i, 0, tierTime);
      }
    } else {
      timesToInvalidateByTier = timesToInvalidateByTierByType[type] = {};
      for (var tier in tiers) {
        timesToInvalidateByTier[tier] = [tiers[tier].floor(time)];
      }
    }
  }

  // Process any deferred metric invalidations, flushing the queues. Note that
  // the queue (timesToInvalidateByTierByType) is copied-on-write, so while the
  // previous batch of events are being invalidated, new events can arrive.
  function flush() {
    var flushed = false;
    for (var type in timesToInvalidateByTierByType) {
      var metrics = collection(type).metrics,
          timesToInvalidateByTier = timesToInvalidateByTierByType[type];
      for (var tier in tiers) {
        metrics.update({
          i: false,
          "_id.l": +tier,
          "_id.t": {$in: timesToInvalidateByTier[tier]}
        }, invalidate, multi);
      }
      flushed = true;
    }
    if (flushed) util.log("flush " + Object.keys(timesToInvalidateByTierByType));
    timesToInvalidateByTierByType = {}; // copy-on-write
  };

  flushInterval = setInterval(flush, flushDelay);

  return putter;
};

exports.getter = function(db) {
  var collection = types(db),
      streamDelay = 5000;

  function getter(request, callback) {
    var stream = !("stop" in request),
        start = new Date(request.start),
        stop = stream ? new Date(Date.now() - streamDelay) : new Date(request.stop);

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
    function query() {
      collection(expression.type).events.find(filter, fields, event_options, function(error, cursor) {
        handle(error);
        cursor.each(function(error, event) {
          if (callback.closed) return cursor.close();
          handle(error);
          if (event) callback({
            time: event.t,
            data: event.d
          });
        });
      });
    }

    query();

    // While streaming, periodically poll for new results.
    if (stream) {
      stream = setInterval(function() {
        if (callback.closed) return clearInterval(stream);
        filter.t.$gte = stop;
        filter.t.$lt = stop = new Date(Date.now() - streamDelay);
        query();
      }, streamDelay);
    }
  }

  getter.close = function(callback) {
    callback.closed = true;
  };

  return getter;
};

function handle(error) {
  if (error) throw error;
}

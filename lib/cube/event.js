'use strict';

// TODO include the event._id (and define a JSON encoding for ObjectId?)
// TODO allow the event time to change when updating (fix invalidation)

var mongodb = require("mongodb"),
    parser = require("./event-expression"),
    tiers = require("./tiers"),
    types = require("./types"),
    bisect = require("./bisect"),
    metalog = require("./metalog"),
    options = require("../../config/cube"),
    ObjectID = mongodb.ObjectID;

var type_re = /^[a-z][a-zA-Z0-9_]+$/,
    invalidate = {$set: {i: true}},
    multi = {multi: true},
    metric_options = options["mongo-metrics"],
    event_options  = options["mongo-events"];

// When streaming events, we should allow a delay for events to arrive, or else
// we risk skipping events that arrive after their event.time. This delay can be
// customized by specifying a `delay` property as part of the request.
var streamDelayDefault = 5000,
    streamInterval = 1000;

// How frequently to invalidate metrics after receiving events.
var invalidateInterval = 5000;

var flushers = [];
exports.stop = function(){ try{
  flushers.forEach(function(flusher){
    metalog.info('cube_life', {is: 'flusher_unregister' });
    clearInterval(flusher);
  }); } catch(err) {}};

// event.putter -- save the event, invalidate any cached metrics impacted by it.
//
// @param request --
//   - id,   a unique ID (optional). If included, it will be used as the Mongo record's primary key -- if the collector receives that event multiple times, it will only be stored once. If omitted, Mongo will generate a unique ID for you.
//   - time, timestamp for the event (a date-formatted string)
//   - type, namespace for the events. A corresponding `foo_events` collection must exist in the DB -- /schema/schema-*.js illustrate how to set up a new event type.
//   - data, the event's payload
//
exports.putter = function(db){
  var collection = types(db),
      knownByType = {},
      eventsToSaveByType = {},
      timesToInvalidateByTierByType = {};

  function putter(request, callback) {
    var time = "time" in request ? new Date(request.time) : new Date(),
        type = request.type;

    // Validate the date and type.
    if (!type_re.test(type)) return callback({error: "invalid type"}), -1;
    if (isNaN(time)) return callback({error: "invalid time"}), -1;

    // // Drop events from before invalidation horizon
    // if (time < new Date(new Date() - options.horizons.invalidation)) return callback({error: "event before invalidation horizon"}), -1;

    // If an id is specified, promote it to Mongo's primary key.
    var event = {t: time, d: request.data};
    if ("id" in request) event._id = request.id;

    // If this is a known event type, save immediately.
    if (type in knownByType) return save(type, event);

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
      handle(error);
      var events = collection(type).events;
      if (names.length) return saveEvents();


      // Create a collection for events. One index is require, for finding events by time(t)
      db.createCollection(type + "_events", event_options, function(error, events){
        handle(error);
        events.ensureIndex({"t": 1}, handle);

        // Create a collection for metrics. Three indexes are required: one
        // for finding metrics, one (_id) for updating, and one for invalidation.
        db.createCollection(type + "_metrics", metric_options, function(error, metrics) {
          handle(error);
          metrics.ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}, handle);
          metrics.ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1}, handle);
          saveEvents();
        });
      });

      // Save any pending events to the new collection.
      function saveEvents() {
        knownByType[type] = true;
        eventsToSaveByType[type].forEach(function(event) { save(type, event); });
        delete eventsToSaveByType[type];
      }
    });
  }

  // Save the event of the specified type, and queue invalidation of any cached
  // metrics associated with this event type and time.
  //
  // We don't invalidate the events immediately. This would cause many redundant
  // updates when many events are received simultaneously. Also, having a short
  // delay between saving the event and invalidating the metrics reduces the
  // likelihood of a race condition between when the events are read by the
  // evaluator and when the newly-computed metrics are saved.
  function save(type, event) {
    // metalog.info("cube_request", { is: "ev", at: "save", type: type, event: event });
    collection(type).events.save(event, handle);
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
        if (i >= tierTimes.length) tierTimes.push(tierTime);
        else if (tierTimes[i] > tierTime) tierTimes.splice(i, 0, tierTime);
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
  flushers.push(setInterval(function() {
    for (var type in timesToInvalidateByTierByType) {
      var metrics = collection(type).metrics,
          timesToInvalidateByTier = timesToInvalidateByTierByType[type];
      metalog.info("cube_compute", { is: "flush", type: type });
      for (var tier in tiers) {
        metrics.update({
          i: false,
          "_id.l": +tier,
          "_id.t": {$in: timesToInvalidateByTier[tier]}
        }, invalidate, multi);
      }
    }
    timesToInvalidateByTierByType = {}; // copy-on-write
  }, invalidateInterval));

  metalog.info('cube_life', {is: 'putter_start'});
  return putter;
};

//
// event.getter - subscribe to event type
//
// if `stop` is not given, does a streaming response, polling for results every
// `streamDelay` (5 seconds).
//
// if `stop` is given, return events from the given interval
//
// * convert the request expression and filters into a MongoDB-ready query
// * Issue the query;
// * if streaming, register the query to be run at a regular interval
//
exports.getter = function(db) {
  var collection = types(db),
      streamsBySource = {};

  function getter(request, callback) {
    var stream = !("stop" in request),
        delay = "delay" in request ? +request.delay : streamDelayDefault,
        start = new Date(request.start),
        stop = stream ? new Date(Date.now() - delay) : new Date(request.stop);

    // Validate the dates.
    if (isNaN(start)) return callback({error: "invalid start"}), -1;
    if (isNaN(stop)) return callback({error: "invalid stop"}), -1;

    // Parse the expression.
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      var resp = { is: "invalid expression", expression: request.expression, error: error };
      metalog.info('cube_getter', resp);
      return callback(resp), -1;
    }

    // Set an optional limit on the number of events to return.
    var options = {sort: {t: -1}, batchSize: 1000};
    if ("limit" in request) options.limit = +request.limit;

    // Copy any expression filters into the query object.
    var filter = {t: {$gte: start, $lt: stop}};
    expression.filter(filter);

    // Request any needed fields.
    var fields = {t: 1};
    expression.fields(fields);

    // Query for the desired events.
    function query(callback) {
      collection(expression.type).events.find(filter, fields, options, function(error, cursor) {
        handle(error);
        cursor.each(function(error, event) {

          // If the callback is closed (i.e., if the WebSocket connection was
          // closed), then abort the query. Note that closing the cursor mid-
          // loop causes an error, which we subsequently ignore!
          if (callback.closed) return cursor.close();

          handle(error);

          // A null event indicates that there are no more results.
          if (event) callback({id: event._id instanceof ObjectID ? undefined : event._id, time: event.t, data: event.d});
          else callback(null);
        });
      });
    }

    // For streaming queries, share streams for efficient polling.
    if (stream) {
      var streams = streamsBySource[expression.source];

      // If there is an existing stream to attach to, backfill the initial set
      // of results to catch the client up to the stream. Add the new callback
      // to a queue, so that when the shared stream finishes its current poll,
      // it begins notifying the new client. Note that we don't pass the null
      // (end terminator) to the callback, because more results are to come!
      if (streams) {
        filter.t.$lt = streams.time;
        streams.waiting.push(callback);
        query(function(event) { if (event) callback(event); });
      }

      // Otherwise, we're creating a new stream, so we're responsible for
      // starting the polling loop. This means notifying active callbacks,
      // detecting when active callbacks are closed, advancing the time window,
      // and moving waiting clients to active clients.
      else {
        streams = streamsBySource[expression.source] = {time: stop, waiting: [], active: [callback]};
        (function poll() {
          query(function(event) {

            // If there's an event, send it to all active, open clients.
            if (event) {
              streams.active.forEach(function(callback) {
                if (!callback.closed) callback(event);
              });
            }

            // Otherwise, we've reached the end of a poll, and it's time to
            // merge the waiting callbacks into the active callbacks. Advance
            // the time range, and set a timeout for the next poll.
            else {
              streams.active = streams.active.concat(streams.waiting).filter(open);
              streams.waiting = [];

              // If no clients remain, then it's safe to delete the shared
              // stream, and we'll no longer be responsible for polling.
              if (!streams.active.length) {
                delete streamsBySource[expression.source];
                return;
              }

              filter.t.$gte = streams.time;
              filter.t.$lt = streams.time = new Date(Date.now() - delay);
              setTimeout(poll, streamInterval);
            }
          });
        })();
      }
    }

    // For non-streaming queries, just send the single batch!
    else query(callback);
  }

  getter.close = function(callback) {
    // results or periodic calls may have already been set in motion, but
    // trigger in the future; this ensures they quit listening and drop further
    // results on the floor.
    callback.closed = true;
  };

  return getter;
};

function handle(error) {
  if (!error) return;
  metalog.info('cube_request', {is: 'event error', error: error });
  throw error;
}

function open(callback) {
  return !callback.closed;
}

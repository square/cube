'use strict';

// TODO include the event._id (and define a JSON encoding for ObjectId?)
// TODO allow the event time to change when updating (fix invalidation)

var _ = require("underscore"),
    mongodb  = require("mongodb"),
    ObjectID = mongodb.ObjectID,
    util     = require("util"),
    tiers    = require("./tiers"),
    Event    = require("./models/event"),
    Metric   = require("./models/metric"),
    parser   = require("./event-expression"),
    bisect   = require("./bisect"),
    metalog  = require("./metalog");

// When streaming events, we should allow a delay for events to arrive, or else
// we risk skipping events that arrive after their event.time. This delay can be
// customized by specifying a `delay` property as part of the request.
var streamDelayDefault = 5000,
    streamInterval = 1000;

// How frequently to invalidate metrics after receiving events.
var invalidateInterval = 5000;

// serial id so we can track flushers
var putter_id = 0;

// event.putter -- save the event, invalidate any cached metrics impacted by it.
//
// @param request --
//   - id,   a unique ID (optional). If included, it will be used as the Mongo record's primary key -- if the collector receives that event multiple times, it will only be stored once. If omitted, Mongo will generate a unique ID for you.
//   - time, timestamp for the event (a date-formatted string)
//   - type, namespace for the events. A corresponding `foo_events` collection must exist in the DB -- /schema/schema-*.js illustrate how to set up a new event type.
//   - data, the event's payload
//

exports.putter = function(db, config){
  var options      = (config || options || {});

  var invalidator = new Invalidator();

  function putter(request, callback){
    var time = "time" in request ? new Date(request.time) : new Date(),
        type = request.type;
    callback = callback || function(){};


    // // Drop events from before invalidation horizon
    // if (time < new Date(new Date() - options.horizons.invalidation)) return callback({error: "event before invalidation horizon"}), -1;

    // // Drop events from before invalidation horizon
    if ((! request.force) && options.horizons && (time < new Date(new Date() - options.horizons.invalidation))) {
      metalog.info('cube_compute', {error: "event before invalidation horizon"});
      return callback({error: "event before invalidation horizon"}), -1;
    }

    var event = new Event(type, time, request.data, request.id);
    try{ event.validate(); } catch(err) { return callback({error: err}), -1; }

    // Save the event, then queue invalidation of its associated cached metrics.
    //
    // We don't invalidate the events immediately. This would cause redundant
    // updates when many events are received simultaneously. Also, having a
    // short delay between saving the event and invalidating the metrics reduces
    // the likelihood of a race condition between when the events are read by
    // the evaluator and when the newly-computed metrics are saved.
    event.save(db, function after_save(error, event){
      if (event) invalidator.add(event.type, event);
      callback(event);
    });
  }

  putter.id = ++putter_id;

  // Process any deferred metric invalidations, flushing the queues. Note that
  // the queue (timesToInvalidateByTierByType) is copied-on-write, so while the
  // previous batch of events are being invalidated, new events can arrive.
  Invalidator.start_flusher(putter.id, function(){
    if (db.isHalted) return putter.stop();
    invalidator.flush(db);
    invalidator = new Invalidator(); // copy-on-write
  });

  putter.invalidator = function(){ return invalidator; };
  putter.stop = function(on_stop){
    metalog.info('putter_stopping', {id: putter.id});
    Invalidator.stop_flusher(putter.id, on_stop);
    invalidator = null
  };

  metalog.info('putter_start', {id: putter.id, inv: invalidator});
  return putter;
};

// --------------------------------------------------------------------------

// Schedule deferred invalidation of metrics by type and tier.
function Invalidator(){
  var type_tsets = {},
      invalidate = { $set: {i: true} },
      multi      = { multi: true };

  this.add = function(type, ev){
    var tt = type_tset(type);
    for (var tier in tiers){ tt[tier][tier*Math.floor(ev.time/tier)] = true; }
  };

  this.flush = function(db){
    _.each(type_tsets, function(type_tset, type){
      db.metrics(type, function(error, collection){
        handle(error);

        _.each(type_tset, function(tset, tier){
          var times = dateify(tset);
          metalog.info("event_flush", { type: type, tier: tier, times: times });
          collection.update({ i: false, "_id.l": +tier, "_id.t": {$in: times}}, invalidate, multi);
        });
      });
    });
  };

  this.tsets = function(){ return _.mapHash(type_tsets, function(tt, type){ return _.mapHash(tt, dateify); }); };
  this._empty = function(){ type_tsets = {}; } // for testing only

  function type_tset(type){
    if (! (type in type_tsets)) type_tsets[type] = _.mapHash(tiers, function(){ return {}; });;
    return type_tsets[type];
  }
  function dateify(tset){ return _.map(_.keys(tset), function(time){ return new Date(+time); }).sort(function(aa,bb){return aa-bb;}); }
}
Invalidator.flushers = {};
Invalidator.start_flusher = function(id, cb){ Invalidator.flushers[id] = setInterval(cb, invalidateInterval); };
Invalidator.stop_flusher  = function(id, on_stop){
  clearInterval(Invalidator.flushers[id]);
  delete Invalidator.flushers[id];
  if (on_stop) on_stop();
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
exports.getter = function(db, config) {
  var options      = (config || options || {}),
      streamsBySource = {};

  function getter(request, callback) {
    var stream = !("stop" in request),
        delay = "delay" in request ? +request.delay : streamDelayDefault,
        start = new Date(request.start),
        stop = stream ? new Date(Date.now() - delay) : new Date(request.stop);

    // Validate the dates.
    if (isNaN(start)) return callback({error: "invalid start"}), -1;
    if (isNaN(stop))  return callback({error: "invalid stop"}),  -1;

    // Parse the expression.
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      var resp = { error: "invalid expression", expression: request.expression, message: error };
      metalog.info('event_getter', resp);
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
      db.events(expression.type, function(error, collection){
        handle(error);
        collection.find(filter, fields, options, function(error, cursor) {
          handle(error);
          cursor.each(function(error, event) {
            // If the callback is closed (i.e., if the WebSocket connection was
            // closed), then abort the query. Note that closing the cursor mid-
            // loop causes an error, which we subsequently ignore!
            if (callback.closed) return cursor.close();

            handle(error);

            // A null event indicates that there are no more results.
            if (event) callback({id: event._id instanceof ObjectID ? undefined : event._id, time: event.t, data: event.d});
            else       callback(null);
          });
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
    // as results or periodic calls trigger in the future, ensure that they quit
    // listening and drop further results on the floor.
    callback.closed = true;
  };

  return getter;
};

function open(callback) {
  return !callback.closed;
}

function handle(error) {
  if (!error) return;
  metalog.error('event', error);
  throw error;
}

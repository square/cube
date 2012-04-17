// TODO include the event._id (and define a JSON encoding for ObjectId?)
// TODO allow the event time to change when updating (fix invalidation)
// TODO fix race condition between cache invalidation and metric computation

var mongodb = require("mongodb"),
    parser = require("./event-expression"),
    tiers = require("./tiers"),
    types = require("./types");

var type_re = /^[a-z][a-zA-Z0-9_]+$/,
    invalidate = {$set: {i: true}},
    multi = {multi: true},
    metric_options = {capped: true, size: 1e7, autoIndexId: true};

// When streaming events, we should allow a delay for events to arrive, or else
// we risk skipping events that arrive after their event.time. This delay can be
// customized by specifying a `delay` property as part of the request.
var streamDelayDefault = 5000,
    streamInterval = 1000;

exports.putter = function(db) {
  var collection = types(db),
      knownByType = {},
      eventsToSaveByType = {};

  function putter(request, callback) {
    var time = new Date(request.time),
        type = request.type;

    // Validate the date and type.
    if (!type_re.test(type)) return callback({error: "invalid type"}), -1;
    if (isNaN(time)) return callback({error: "invalid time"}), -1;

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
      var events = collection(type).events;
      if (names.length) return saveEvents();

      // Events are indexed by time.
      events.ensureIndex({"t": 1}, handle);

      // Create a capped collection for metrics. Three indexes are required: one
      // for finding metrics, one (_id) for updating, and one for invalidation.
      db.createCollection(type + "_metrics", metric_options, function(error, metrics) {
        handle(error);
        metrics.ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}, handle);
        metrics.ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1}, handle);
        saveEvents();
      });

      // Save any pending events to the new collection.
      function saveEvents() {
        knownByType[type] = true;
        eventsToSaveByType[type].forEach(function(event) { save(type, event); });
        delete eventsToSaveByType[type];
      }
    });
  }

  // Save the event of the specified type, and invalidate any cached metrics
  // associated with this event type and time.
  function save(type, event) {
    type = collection(type);
    type.events.save(event, handle);
    for (var tier in tiers) {
      type.metrics.update({
        i: false,
        "_id.l": +tier,
        "_id.t": tiers[tier].floor(event.t)
      }, invalidate, handle);
    }
  }

  return putter;
};

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
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      return callback({error: "invalid expression"}), -1;
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
          if (event) callback({time: event.t, data: event.d});
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
          query(function callback(event) {

            // If there's an event…
            if (event) {
              var closed = false;

              // Send the event to all active, open clients.
              streams.active.forEach(function(callback) {
                if (!callback.closed) callback(event);
                else closed = true;
              });

              // Remove any closed callbacks.
              // Removal is rare, so we don't want to filter every time.
              if (closed) streams.active = streams.active.filter(open);

              // If no clients remain, then it's safe to close the callback.
              // The query function will then terminate the underlying cursor.
              if (!streams.active.length && !streams.waiting.length) {
                callback.closed = true;
                delete streamsBySource[expression.source];
              }
            }

            // Otherwise, we've reached the end of a poll, and it's time to
            // merge the waiting callbacks into the active callbacks. Advance
            // the time range, and set a timeout for the next poll.
            else {
              streams.active = streams.active.concat(streams.waiting);
              streams.waiting = [];
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
    callback.closed = true;
  };

  return getter;
};

function handle(error) {
  if (error) throw error;
}

function open(callback) {
  return !callback.closed;
}

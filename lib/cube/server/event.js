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
    event_options = {sort: {t: -1}, batchSize: 1000},
    metric_options = {capped: true, size: 1e7, autoIndexId: true};

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
      streamDelay = 5000;

  function getter(request, callback) {
    var stream = !("stop" in request),
        start = new Date(request.start),
        stop = stream ? new Date(Date.now() - streamDelay) : new Date(request.stop);

    // Validate the dates.
    if (isNaN(start)) return callback({error: "invalid start"}), -1;
    if (isNaN(stop)) return callback({error: "invalid stop"}), -1;

    // Parse the expression.
    var expression;
    try {
      expression = parser.parse(request.expression);
    } catch (error) {
      return callback({error: "invalid expression"}), -1;
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
          if (event) callback({time: event.t, data: event.d});
          else if (!stream) callback(null);
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

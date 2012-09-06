'use strict';

var mongodb        = require("mongodb"),
    metalog        = require("./metalog"),
    metric_options, event_options,
    database, events_db, metrics_db;

module.exports = {
  open:        open,
  isConnected: isConnected
}


//
// Connect to mongodb.
//

function open(options, callback){
  var server_options = options["mongo-server_options"], // MongoDB server configuration
      db_options     = { native_parser: true }, // MongoDB driver configuration.
      mongo          = new mongodb.Server(options["mongo-host"], options["mongo-port"], server_options),
      database_name  = options["mongo-database"],
      mongo_password = options["mongo-password"];

  metric_options = options["mongo-metrics"],
  event_options  = options["mongo-events"],
  database       = new mongodb.Db(database_name, mongo, db_options);

  delete options["mongo-password"];
  metalog.info('cube_life', {is: 'mongo_connect', options: options });

  database.open(function(error){
    if (error) return callback(error);

    // Open separate events database if configured
    if (options["separate-events-database"])  events_db  = database.db(database_name + '-events');
    else events_db = database;

    // Open separate metrics database if configured
    if (options["separate-metrics-database"]) metrics_db = database.db(database_name + '-metrics');
    else metrics_db = database;

    module.exports.metrics    = metrics(metrics_db);
    module.exports.events     = events(events_db);
    module.exports.types      = types(events_db);
    module.exports.collection = collection(database);
    module.exports.close      = close;

    if (! options["mongo-username"]) return callback(null, module.exports);
    database.authenticate(options["mongo-username"], mongo_password, function(error, success) {
      if (error) return callback(error);
      if (!success) return callback(new Error("authentication failed"));
      callback(null, module.exports);
    });
  });
}


//
// Close connection to mongodb.
//

function close(callback){
  delete module.exports.metrics,
  delete module.exports.events,
  delete module.exports.types,
  delete module.exports.collection,
  delete module.exports.close;

  database.close(callback);
}

function isConnected(){
  try {
    return database.serverConfig.isConnected();
  } catch (e){
    return false;
  }
}


// Much like db.collection, but caches the result for both events and metrics.
// Also, this is synchronous, since we are opening a collection unsafely.
function metrics(database) {
  var collections = {};

  return function(name, callback){
    if(collections[name]) return callback(null, collections[name]);

    var collection_name = name + "_metrics",
        _this = this;
    database.createCollection(collection_name, metric_options||{safe: false}, function(error, metrics) {

      if (error && error.errmsg == "collection already exists") return _this.metrics(name, callback);
      if (error) return callback(error);

      metrics.ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}, handle);
      metrics.ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1}, handle);

      metrics = overwrite_find(metrics, 'metrics');
      collections[name] = metrics;
      return callback(null, metrics);
    });
  }
}

function events(database) {
  var collections = {};

  return function(name, callback){
    if (collections[name]) return callback(null, collections[name]);

    var collection_name = name + "_events";

    // Create a collection for events. One index is require, for finding events by time(t)
    database.createCollection(collection_name, event_options||{safe: false}, function(error, events){

      if (error && error.errmsg == "collection already exists") return _this.metrics(name, callback);
      if(error) return callback(error);

      events.ensureIndex({"t": 1}, handle);

      events = overwrite_find(events, 'events');
      collections[name] = events;
      return callback(null, events);
    });
  }
}

function collection(database){
  var collections = {};

  return function(name, callback){
    if (collections[name]) return callback(null, collections[name]);
    database.collection.apply(database, arguments);
  }
}

function overwrite_find(collection, type){
  var orig_find = collection.find;
  collection.find = function(){
    metalog.minor('cube_query', {is: type + '_find', query: arguments});
    return orig_find.apply(this, arguments);
  };
  return collection;
}

var eventRe = /_events$/;

function types(database) {
  return function(request, callback) {
    database.collectionNames(function(error, names) {
      handle(error);
      callback(names
        .map(function(d)    { return d.name.split(".")[1];        })
        .filter(function(d) { return eventRe.test(d);              })
        .map(function(d)    { return d.substring(0, d.length - 7); })
        .sort());
    });
  };
};

function handle(error){
  if (!error) return;
  metalog.info('cube_request', {is: 'db error', error: error });
  throw error;
}

function noop(){};

'use strict';

var util           = require("util"),
    mongodb        = require("mongodb"),
    metalog        = require("./metalog"),
    metric_options, event_options,
    database, events_db, metrics_db;

var db = {
  open:        open,
  isConnected: isConnected
}

var collections = {};

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

    db.metrics    = metrics_collection_factory(metrics_db);
    db.events     = events_collection_factory(events_db);
    db.types      = types(events_db);
    db.collection = collection(database);
    db.close      = close;

    if (! options["mongo-username"]) return callback(null, db);
    
    database.authenticate(options["mongo-username"], mongo_password, function(error, success) {
      if (error) return callback(error);
      if (!success) return callback(new Error("authentication failed"));
      callback(null, db);
    });
  });
}


//
// Close connection to mongodb.
//

function close(callback){
  collections = {};
  delete db.metrics;
  delete db.events;
  delete db.types;
  delete db.collection;
  delete db.close;
  if (isConnected()){ database.close(callback); } else { callback(null); }
}

function isConnected(){
  try {
    return database.serverConfig.isConnected();
  } catch (e){
    return false;
  }
}

// // Much like db.collection, but caches the result for both events and metrics.
// // Also, this is synchronous, since we are opening a collection unsafely.
// function metrics(database) {
//   var m_collections = {};
// 
//   return function(name, callback){
//     if(m_collections[name]) return callback(null, m_collections[name]);
// 
//     var collection_name = name + "_metrics",
//         _this = this;
//     database.createCollection(collection_name, metric_options||{safe: false}, function(error, metrics) {
// 
//       if (error && error.errmsg == "collection already exists") return _this.metrics(name, callback);
//       if (error) return callback(error);
// 
//       metrics = logging_clxn(metrics, 'metrics');
//       m_collections[name] = metrics;
//       return callback(null, metrics);
//     });
//   }
// }

function metrics_collection_factory(database){
  return function metrics(name, on_collection, tr){
    return clxn_for(database, (name + "_metrics"), (metric_options||{safe: false}), on_collection, on_m_create, tr);
    function on_m_create(clxn, oc_cb){
      clxn.ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}, handle);
      clxn.ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1}, function(error){ oc_cb(error, clxn); });
    }
  };
}

function events_collection_factory(database){
  return function events(name, on_collection, tr){
    return clxn_for(database, (name + "_events"), (event_options||{safe: false}), on_collection, on_e_create, tr);
    function on_e_create(clxn, oc_cb){
      db.metrics(name, function(error){
        if (error) return oc_cb(error);
        clxn.ensureIndex({"t": 1}, function(error){ oc_cb(error, clxn); });
      });
    }
  };
}

var pending_callbacks = {};

function  clxn_for(database, clxnname, clxnopts, on_collection, on_create, tr){
  on_collection = on_collection || function(){};
  // If we've cached the collection, call back immediately
  if (collections[clxnname]) return on_collection(null, collections[clxnname]);

  // If someone is already creating the collection for this new type,
  // then append the callback to the queue for later save.
  if (clxnname in pending_callbacks){
    metalog.trace('clxnQ', tr);
    return pending_callbacks[clxnname].push(on_collection);
  }

  // Otherwise, it's up to us to create the corresponding collection, then save
  // any requests that have queued up in the interim!

  // First add the new event to the queue.
  pending_callbacks[clxnname] = [on_collection];
  
  // Create collection, and issue callback for index creation, etc
  database.createCollection(clxnname, clxnopts, function(error, clxn){
    metalog.minor('create collection', {name: clxnname});
    if (error && (error.errmsg == "collection already exists")) return db.collection(clxnname, adopt_collection);
    if (!error) return on_create(clxn, adopt_collection);
    if (error)  return adopt_collection(error);
  });

  function adopt_collection(error, clxn){
    if (! error){
      clxn = logging_clxn(clxn, clxnname);
      collections[clxnname] = clxn;
    }
    pending_callbacks[clxnname].forEach(function(cb){ cb(error, clxn) });
    delete pending_callbacks[clxnname];
  }
}

function collection(database){
  return function(name, callback){
    if (collections[name]) return callback(null, collections[name]);
    database.collection.apply(database, arguments);
  }
}

function logging_clxn(collection, type){
  var orig_find = collection.find;
  collection.find = function(){
    //metalog.minor((type + '_find'), {query: arguments});
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
  metalog.error('db error', error);
  throw error;
}

function noop(){};

module.exports = db;

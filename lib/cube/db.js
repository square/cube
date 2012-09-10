'use strict';

var util     = require("util"),
    mongodb  = require("mongodb"),
    metalog  = require("./metalog"),
    db_index = 0;

function Db(){
  var metric_options, event_options, db_client, events_client, metrics_client;
  var db_id = db_index++;

  var db = this;
  db.open  = open;
  db.close = close;
  db.isConnected = isConnected;

  var collections       = {}; // cache of collection handles
  var pending_callbacks = {}; // callbacks waiting for collections to be found
  var collection_prefix = '';

  this.report = function report(){ return { idx: db_id, isHalted: db.isHalted }; };

  //
  // Connect to mongodb.
  //

  function open(options, callback){
    var server_options = options["mongo-server_options"], // MongoDB server configuration
    db_options     = { native_parser: true },         // MongoDB driver configuration.
    mongo          = new mongodb.Server(options["mongo-host"], options["mongo-port"], server_options),
    database_name  = options["mongo-database"],
    mongo_password = options["mongo-password"];

    if (db_client) { metalog.minor('mongo_already_open', db.report()); return callback(null, db); }

    collection_prefix = options["collection_prefix"] || '';
    metric_options = options["mongo-metrics"],
    event_options  = options["mongo-events"],
    db_client       = new mongodb.Db(database_name, mongo, db_options);

    delete options["mongo-password"];
    metalog.info('mongo_connect', db.report());

    db_client.open(function(error){
      if (error) return callback(error);

      // Open separate events database if configured
      if (options["separate-events-database"])  events_client  = db_client.db(database_name + '-events');
      else events_client = db_client;

      // Open separate metrics database if configured
      if (options["separate-metrics-database"]) metrics_client = db_client.db(database_name + '-metrics');
      else metrics_client = db_client;

      db.metrics    = metrics_collection_factory(metrics_client);
      db.events     = events_collection_factory(events_client);
      db.types      = types(events_client);
      db.collection = collection(db_client);

      if (! options["mongo-username"]) return callback(null, db);
      
      db_client.authenticate(options["mongo-username"], mongo_password, function(error, success) {
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
    metalog.trace('mongo_closing', db.report());
    if (! db_client) return metalog.minor('mongo_already_closed');
    collections = {};
    delete db.metrics;
    delete db.events;
    delete db.types;
    delete db.collection;
    if (isConnected()){ db_client.close(); }
    db_client = events_client = metrics_client = null;
    db.isHalted = true;

    return callback(null);
  }
  db.isHalted = false;

  function isConnected(){
    try {
      return db_client.serverConfig.isConnected();
    } catch (e){
      return false;
    }
  }

  function metrics_collection_factory(client){
    return function metrics(name, on_collection, tr){
      var clxnname = (collection_prefix + name + "_metrics");
      return clxn_for(client, clxnname, (metric_options||{safe: false}), on_collection, on_m_create, tr);
      function on_m_create(clxn, oc_cb){
        clxn.ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1}, handle);
        clxn.ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1}, function(error){ oc_cb(error, clxn); });
      }
    };
  }

  function events_collection_factory(client){
    return function events(name, on_collection, tr){
      var clxnname = (collection_prefix + name + "_events");
      return clxn_for(client, clxnname, (event_options||{safe: false}), on_collection, on_e_create, tr);
      function on_e_create(clxn, oc_cb){
        db.metrics(name, function also_make_metrics_clxn(error){
          if (error) return oc_cb(error);
          clxn.ensureIndex({"t": 1}, function(error){ oc_cb(error, clxn); });
        });
      }
    };
  }

  function  clxn_for(client, clxnname, clxnopts, on_collection, on_create, tr){
    on_collection = on_collection || function(){};
    
    // If we've cached the collection, call back immediately
    if (collections[clxnname]) return on_collection(null, collections[clxnname]);
    
    // If someone is already creating the collection for this new type,
    // then append the callback to the queue for later save.
    if (clxnname in pending_callbacks){
      metalog.trace('clxnQ', tr, {clxnname: clxnname, id: db_id});
      return pending_callbacks[clxnname].push(on_collection);
    }

    // Otherwise, it's up to us to create the corresponding collection, then save
    // any requests that have queued up in the interim!

    // First add the new event to the queue.
    pending_callbacks[clxnname] = [on_collection];
    
    // Create collection, and issue callback for index creation, etc
    client.createCollection(clxnname, clxnopts, function(error, clxn){
      metalog.trace('clxnC', tr);
      if (error && (error.errmsg == "collection already exists")){
        metalog.trace('clxnE', tr);
        return client.collection(clxnname, adopt_collection);
      }
      if (!error) return on_create(clxn, adopt_collection);
      if (error)  return adopt_collection(error);
    });

    function adopt_collection(error, clxn){
      var callbacks = pending_callbacks[clxnname];
      delete pending_callbacks[clxnname];

      metalog.trace('clxnD', tr);
      if (! error){
        clxn = logging_clxn(clxn, clxnname);
        collections[clxnname] = clxn;
      }
      callbacks.forEach(function(cb){
        try{          cb(error, clxn); }
        catch(error){ metalog.error('db_pending_callbacks', error); }
      });
    }
  }

  function collection(client){
    return function(name, callback){
      if (collections[name]) return callback(null, collections[name]);
      client.collection.apply(client, arguments);
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

  function types(client) {
    return function(request, callback) {
      client.collectionNames(function(error, names) {
        handle(error);
        callback(names
                 .map(function(d)    { return d.name.split(".")[1];        })
                 .filter(function(d) { return eventRe.test(d);              })
                 .map(function(d)    { return d.substring(0, d.length - 7); })
                 .sort());
      });
    };
  };

}

function handle(error){
  if (!error) return;
  metalog.error('db error', error);
  throw error;
}

function noop(){};

module.exports = Db;

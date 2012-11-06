var config = require('./config/cube'),
    Db     = require('./lib/cube/db');

namespace("db", function(){
  namespace("metrics", function(){
    desc("Remove metrics with times past the forced metric expiration horizon")
    task("remove_expired", [], function(){
      if(!config.horizons.forced_metric_expiration){
        throw new Error("horizons.forced_metric_expiration MUST be set in: config/cube.js");
      }

      var db = new Db(),
          expiration_date = new Date(new Date() - config.horizons.forced_metric_expiration);

      function handle(err) {
        if(err) throw err;
      }

      db.open(config, function(err, db){
        handle(err);
        var metrics_db = db._dbs.metrics;
        metrics_db.collectionNames({namesOnly: true}, function(err, names){
          handle(err);
          var metric_names = names.filter(function(name){ return /_metrics$/.test(name); }),
              remaining    = metric_names.length;
          metric_names.forEach(function(name){
              var segments = name.split('.'),
                  collection_name = (segments.shift(), segments.join('.'));
              metrics_db.collection(collection_name, function(err, collection){
                handle(err);
                collection.remove({'_id.t': {$lt: expiration_date }}, function(err){
                  handle(err);
                  console.log('Removing ' + collection_name.split('_').join(' ') + ' older than ' + expiration_date);
                  if(!--remaining) db.close();
                });
              });
            });
        })
      })
    });
  });
});
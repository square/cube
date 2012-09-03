'use strict';

var cluster = require('cluster'),
    mongodb = require('mongodb'),
    metric  = require('./metric'),
    tiers   = require('./tiers'),
    metalog = require('./metalog')

module.exports = function(options){
  var db, mongo, calculate_metric, boards, tier;

  function fetch_metrics(callback){
    var expressions = [];

    if(!boards){
      db.collection("boards", function(error, collection) { boards = collection; fetch_metrics(callback); });
      return;
    }

    boards.find({}, {pieces: 1}, function(error, cursor) {
      if (error) throw error;
      cursor.each(function(error, row) {
        if (error) throw error;
        if (row) {
          expressions.splice.apply(expressions, [0, 0].concat(row.pieces
            .map(function(piece){ return piece.query; })
            .filter(function(expression){ return expression && !(expression in expressions); })
          ));
        } else {
          callback(expressions);
        }
      });
    });
  }

  function process_metrics(expressions){
    expressions.forEach(function(expression){
      var stop  = new Date(),
          start = tier.step(tier.floor(new Date(stop - options.horizons.calculation)));

      metalog.info('cube_warm', {is: 'warm_metric', metric: {query: expressions}, start: start, stop: stop });

      // fake metrics request
      calculate_metric({ step: tier.key, expression: expression, start: start, stop: stop }, function(){});
    });
    setTimeout(function(){ fetch_metrics(process_metrics); }, options['warmer-interval']);
  }

  return {
    start: function(){
      mongo   = new mongodb.Server(options['mongo-host'], options['mongo-port']);
      db      = new mongodb.Db(options["mongo-database"], mongo),
      tier    = tiers[options['warmer-tier'].toString()];

      if(typeof tier === "undefined") throw new Error("Undefined warmer tier configured: " + options['warmer-tier']);

      metalog.event("cube_life", { is: 'start_warmer', options: options });

      db.open(function(error) {
        if (error) throw error;
        calculate_metric = metric.getter(db);
        fetch_metrics(process_metrics);
      });
    }
  };
}

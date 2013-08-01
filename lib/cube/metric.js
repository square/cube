'use strict';

// TODO use expression ids or hashes for more compact storage

var _ = require("underscore"),
    util    = require("util"),
    queuer  = require("queue-async"),
    parser  = require("./metric-expression"),
    tiers   = require("./tiers"),
    reduces = require("./reduces"),
    Metric =  require("./models/metric"),
    Measurement = require("./models/measurement"),
    event   = require("./event"),
    metalog = require('./metalog');

// Query for metrics.

exports.getter = function(db) {
  var streamsBySource = {};

  function getter(request, callback) {
    var measurement, expression,
        tier  = tiers[+request.step],
        start = new Date(request.start),
        stop  = new Date(request.stop);

    try {
      if (!tier)        throw "invalid step";
      if (isNaN(start)) throw "invalid start";
      if (isNaN(stop))  throw "invalid stop";

      // Round start and stop to the appropriate time step.
      start       = tier.floor(start);
      stop        = tier.ceil(stop);
      expression  = parser.parse(request.expression);
      measurement = new Measurement(expression, start, stop, tier);

      measurement.on('complete', function(){ callback(new Metric({time: stop, value: null}, measurement)); });
    } catch(error) {
      metalog.error('mget', error, { info: util.inspect([start, stop, tier, expression] )});
      return callback({error: error, _trace: request._trace}), -1;
    }



    measurement.measure(db, callback);
  }

  return getter;
};

function handle(error) {
  if (!error) return;
  metalog.error('metric', error);
  throw error;
}

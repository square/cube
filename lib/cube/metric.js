'use strict';

// TODO use expression ids or hashes for more compact storage

var _ = require("underscore"),
    util    = require("util"),
    queuer  = require("../queue-async/queue"),
    parser  = require("./metric-expression"),
    tiers   = require("./tiers"),
    reduces = require("./reduces"),
    Metric =  require("./models/metric"),
    Measurement = require("./models/measurement"),
    event   = require("./event"),
    metalog = require('./metalog'),
    options = require('../../config/cube');

// Query for metrics.

exports.getter = function(db, config) {
  var options      = (config || options || {});

  function getter(request, callback) {
    var measurement, expression,
        tier  = tiers[+request.step],
        start = new Date(request.start),
        stop  = new Date(request.stop);

    try {
      if (!tier)        throw "invalid step";
      if (isNaN(start)) throw "invalid start";
      if (isNaN(stop))  throw "invalid stop";
      // if (request.expression && (request.expression.match(/\(/mg).length > 2)){ throw("rejected complex expression"); }

      // Round start and stop to the appropriate time step.
      start       = tier.floor(start);
      stop        = tier.ceil(stop);
      expression  = parser.parse(request.expression);
      measurement = new Measurement(expression, start, stop, tier);

      measurement.on('complete', function(){ handle_response(new Metric(stop, null, measurement)); });
    } catch(error) {
      metalog.error('mget', error, { info: util.inspect([start, stop, tier, expression] )});
      return callback({error: error, _trace: request._trace}), -1;
    }

    function handle_response(metric){
      callback(metric);
      if (metric.value || metric.value === 0) metric.save(db, handle);
    }

    measurement.measure(db, options, handle_response);
  }

  return getter;
};

function handle(error) {
  if (!error) return;
  metalog.error('metric', error);
  throw error;
}

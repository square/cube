'use strict';

var _ = require("underscore"),
    metalog    = require('../metalog'),
    mongo      = require('mongodb'),
    Model      = require("../core_ext/model");

var second   = 1e3,
    second10 = 10e3,
    minute   = 60e3,
    minute5  = 300e3,
    hour     = 3600e3,
    day      = 86400e3;

var tiers  = require("../tiers"),
    tensec = tiers[second10],
    type_re = /^[a-z][a-zA-Z0-9_]+$/,
    metric_fields = {v: 1, vs: 1},
    metric_options = {sort: {"_id.t": 1}, batchSize: 1000};

function Metric(time, value, measurement, values){
  this.time  = time;
  this.value = value;

  this.setProperty("values", { value: values||[] });
  this.setProperty("measurement", { value: measurement });
}

Model.modelize(Metric);

Metric.setProperties({
  tier:    { get: function(){ return this.measurement.tier } },
  bin:     { get: function(){ return this.tier.bin(this.time); } },
  e:       { get: function(){ return this.measurement.expression.source }},
  l:       { get: function(){ return this.measurement.tier.key }},
  type:    { get: function(){ return this.measurement.expression.type }},

  to_wire: { value: to_wire },
  report:  { value: report  },
  save:    { value: save }

  // day_bin: function(){ return tiers[day     ].bin(this.t); },
  // m05_bin: function(){ return tiers[minute5 ].bin(this.t); },
  // s10_bin: function(){ return tiers[second10].bin(this.t); },
  // day_ago: function day_ago(){ return Math.floor((Date.now() - this.t) / day);      },
  // m05_ago: function m05_ago(){ return Math.floor((Date.now() - this.t) / minute5);  },
  // s10_ago: function s10_ago(){ return Math.floor((Date.now() - this.t) / second10); },
  // bins:    function bin(){ return [this.day_bin(), this.m05_bin(), this.s10_bin() ]; },
  // agos:    function ago(){ return [this.day_ago(), this.m05_ago(), this.s10_ago() ]; },
});

function find(db, measurement, callback){
  var expression = measurement.expression,
      start = measurement.start,
      stop  = measurement.stop,
      type  = expression.type,
      tier  = measurement.tier;

  db.metrics(type, function(error, collection){
    if (error) return callback(error);

    var query = {
      i: false,
      "_id.e": expression.source,
      "_id.l": tier.key,
      "_id.t": {
        $gte: start,
        $lt: stop
      }
    };
    collection.find(query, metric_fields, metric_options, handleResponse);
  });

  function handleResponse(error, cursor){
    if (error) return callback(error);
    cursor.each(function(error, row) {
      if (error) return callback(error);
      if (row) callback(error, Metric.from_wire(row, measurement));
      else callback();
    })
  }
}
Object.defineProperty(Metric, "find", { value: find });
Object.defineProperty(Metric, "from_wire", { value: from_wire });

function from_wire(row, measurement){
  var values = row.vs.reduce(function(expanded, value){
    _.times(value.c, function(){ expanded.push(value.v); });
    return expanded;
  }, []);
  return new Metric(row._id.t, row.v, measurement, values);
}

function to_wire(){
  var values = this.values.reduce(function(values, value){
    var pair = _.find(values, function(pair){ return pair.value == value; });
    if (!pair) values.push(pair = { v: value, c: 0 });
    pair.c++;
    return values;
  }, []);
  return { i: false, v: mongo.Double(this.value), vs: values, _id: { e: this.e, l: this.l, t: this.time } };
};

function report(){
  var hsh = { time: this.time, value: this.value };
  return hsh;
};

function save(db, callback){
  var self = this;
  if (this.validate) this.validate();

  db.metrics(self.type, function(error, collection){
    if (error) return callback(error);
    collection.save(self.to_wire(), function(error){
      callback(error, self);
    });
  });
};

module.exports = Metric;

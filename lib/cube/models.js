'use strict';

var _ = require("underscore"),
    metalog    = require('./metalog');

var second   = 1e3,
    second10 = 10e3,
    minute   = 60e3,
    minute5  = 300e3,
    hour     = 3600e3,
    day      = 86400e3;
exports.units = { second: second, second10: second10, minute: minute, minute5: minute5, hour: hour, day: day };

var tiers  = require("./tiers"),
    tensec = tiers[second10],
    type_re = /^[a-z][a-zA-Z0-9_]+$/;

_.mapHash = function(obj, func){
  var res = {};
  _.each(obj, function(val, key){ res[key] = func(val, key, res); });
  return res;
};

function Event(type, time, data, id){
  this.t    = time;
  this.d    = data;
  this.type = type;
  if (id) this._id = id;

  var self = this;
 
  if (this._id) Object.defineProperty(this, 'to_wire', { value: { t: time, d: data, _id: _id }, enumerable: false, writable: false });
  else          Object.defineProperty(this, 'to_wire', { value: { t: time, d: data           }, enumerable: false, writable: false });
  
  Object.defineProperties(this, {
    _trace: { value: null, enumerable: false, writable: true }
  });
}
Event.prototype = {
  bin: function(tr){ return tiers[tr].bin(this.t); },
  day_bin: function(){ return tiers[day     ].bin(this.t); },
  m05_bin: function(){ return tiers[minute5 ].bin(this.t); },
  s10_bin: function(){ return tiers[second10].bin(this.t); },
  day_ago: function day_ago(){ return Math.floor((Date.now() - this.t) / day);      },
  m05_ago: function m05_ago(){ return Math.floor((Date.now() - this.t) / minute5);  },
  s10_ago: function s10_ago(){ return Math.floor((Date.now() - this.t) / second10); },
  bins:    function bin(){ return [this.day_bin(), this.m05_bin(), this.s10_bin() ]; },
  agos:    function ago(){ return [this.day_ago(), this.m05_ago(), this.s10_ago() ]; },
  report: function report(){
    return { time: this.t, type: this.type, bin: this.bins(), ago: this.agos() };
  }
};

Event.prototype.save = function(db, callback){
  var self = this;
  // metalog.trace('eSva', self);
  db.events(self.type, function event_saver(error, collection){
    if (error) return callback(error);
    // metalog.trace('eSvc', self);
    collection.save(self.to_wire, function saver(error){
      // metalog.trace('eSvd', self);
      callback(error, self, self);
    }); }, self);
};

// Validate the date and type.
Event.prototype.validate = function(){
  if (!type_re.test(this.type)) throw("invalid type");
  if (isNaN(this.t))            throw("invalid time");
};

Event.prototype.to_request = function(attrs){
  var ev = { time: this.t, data: this.d, type: this.type };
  if (this._id) ev.id = this._id;
  for (var key in attrs){ ev[key] = attrs[key]; }
  metalog.trace('new_event', ev);
  return ev;
};

exports.Event = Event;

// --------------------------------------------------------------------------

function Metric(time, value, id, measurement){
  this.time  = time;
  this.value = value;
  if (id) this.id = id;
  
  Object.defineProperties(this, {
    e:           { value: measurement.expression.source, enumerable: false, writable: false },
    l:           { value: measurement.tier.key,          enumerable: false, writable: false },
    measurement: { value: measurement, enumerable: false, writable: false },
    _trace:      { value: null, enumerable: false, writable: true },
  });
}
Object.defineProperties(Metric.prototype, {
  tier:    { get: function(){ return this.measurement.tier } },
  bin:     { get: function(){ return this.tier.bin(this.time); } },
  // day_bin: function(){ return tiers[day     ].bin(this.t); },
  // m05_bin: function(){ return tiers[minute5 ].bin(this.t); },
  // s10_bin: function(){ return tiers[second10].bin(this.t); },
  // day_ago: function day_ago(){ return Math.floor((Date.now() - this.t) / day);      },
  // m05_ago: function m05_ago(){ return Math.floor((Date.now() - this.t) / minute5);  },
  // s10_ago: function s10_ago(){ return Math.floor((Date.now() - this.t) / second10); },
  // bins:    function bin(){ return [this.day_bin(), this.m05_bin(), this.s10_bin() ]; },
  // agos:    function ago(){ return [this.day_ago(), this.m05_ago(), this.s10_ago() ]; },
  // report: function report(){
  //   return { time: this.t, type: this.type, bin: this.bins(), ago: this.agos() };
  // }
});

Metric.prototype.to_wire = function to_wire(){
  return { i: false, v: this.value, _id: { e: this.e, l: this.l, t: time } };
};

Metric.prototype.report = function report(){
  var hsh = { time: this.time, value: this.value };
  if (this.id) hsh.id = this.id;
  return hsh;
};

exports.Metric = Metric;

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
}


function Event(type, time, data, id){
  this.t    = time;
  this.d    = data;
  if (id) this._id = id;

  this._type = function(){ return type; };

  this.bin = function(tr){ return tiers[tr].bin(this.t); };

  this.day_bin = function(){ return tiers[day     ].bin(this.t); };
  this.m05_bin = function(){ return tiers[minute5 ].bin(this.t); };
  this.s10_bin = function(){ return tiers[second10].bin(this.t); };

  this.day_ago = function day_ago(){ return Math.floor((Date.now() - this.t) / day);      }
  this.m05_ago = function m05_ago(){ return Math.floor((Date.now() - this.t) / minute5);  }
  this.s10_ago = function s10_ago(){ return Math.floor((Date.now() - this.t) / second10); }

  this.bins    = function bin(){ return [this.day_bin(), this.m05_bin(), this.s10_bin() ]; }
  this.agos    = function ago(){ return [this.day_ago(), this.m05_ago(), this.s10_ago() ]; }
  this.report = function report(){
    return { time: this.t, type: this.type, bin: this.bins(), ago: this.agos() };
  }

  this.validate = function(){
    // Validate the date and type.
    if (!type_re.test(type)) throw("invalid type");
    if (isNaN(time))         throw("invalid time");
  }

  this.to_wire = function(){
    var ev = { t: this.t, d: this.d };
    if (id) ev._id = this._id;
    return ev;
  }
  this.to_request = function(attrs){
    var ev = { time: this.t, data: this.d, type: type };
    if (id) ev.id = this._id;
    for (var key in attrs){ ev[key] = attrs[key]; };
    return ev;
  };
}
exports.Event = Event;

function Metric(type, time, tier, expression, value){

  // this.type = type;
  // this.time = time;
  // this.tier = tier;
  // this.expression = expression;
  // if (id) this._id = id;

  this.to_wire = function to_wire(){
    return {
      _id: {
        e: expression.source,
        l: tier.key,
        t: time
      },
      i: false,
      v: value
    }
  };
}
exports.Metric = Metric;

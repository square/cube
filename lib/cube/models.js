'use strict';

function Event(type, time, data, id){
  this.type = function(){ return type; }
  this.t    = time;
  this.d    = data;
  if (id) this._id = id;
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

exports.units = {
  second:      1e3,
  second10:   10e3,
  minute:     60e3,
  minute5:   300e3,
  hour:     3600e3,
  day:     86400e3
};

'use strict';

var _ = require("underscore"),
    metalog    = require('../metalog');

var tiers  = require("../tiers"),
    Model  = require("../core_ext/model"),
    tensec = tiers[tiers.units['second10']],
    type_re = /^[a-z][a-zA-Z0-9_]+$/,
    event_options = {sort: {t: 1}, batchSize: 1000};

_.mapHash = function(obj, func){
  var res = {};
  _.each(obj, function(val, key){ res[key] = func(val, key, res); });
  return res;
};

function formatData(data){
  if (!_.isObject(data)) return data;
  if (Array.isArray(data)) return data.map(formatData);
  _.keys(data).forEach(function(key){
    data[key] = formatData(data[key]);

    if (_.isNumber(key) || /^[0-9]$/.test(key)) {
      var val = data[key]; delete data[key];
      data['k' + key] = val;
    }
  });
  return data;
}

function Event(type, time, data, id){
  this.time    = time;
  this.data    = formatData(data);
  if (id) this.id = id;

  this.setProperty("type", { value: type });
}

Model.modelize(Event);

Event.setProperties({
  bin:     { value: function(tr){ return tiers[tr].bin(this.time); }},
  day_bin: { value: function(){ return tiers[day     ].bin(this.time); }},
  m05_bin: { value: function(){ return tiers[minute5 ].bin(this.time); }},
  s10_bin: { value: function(){ return tiers[second10].bin(this.time); }},
  day_ago: { value: function day_ago(){ return Math.floor((Date.now() - this.time) / day);      }},
  m05_ago: { value: function m05_ago(){ return Math.floor((Date.now() - this.time) / minute5);  }},
  s10_ago: { value: function s10_ago(){ return Math.floor((Date.now() - this.time) / second10); }},
  bins:    { value: function bins(){ return tiers.bins(this.time); }},
  agos:    { value: function agos(){ return [this.day_ago(), this.m05_ago(), this.s10_ago() ]; }},
  report:  { value: function report(){
    return { time: this.time, type: this.type, bin: this.bins(), ago: this.agos() };
  }},

  to_wire: { value: function(){ var event = { t: this.time, d: this.data, b: this.bins() }; if (this.id) event._id = this.id; return event; }},

  save:       { value: save },
  validate:   { value: validate },
  to_request: {value: to_request }
});

function find(db, measurement, callback){
  var expression = measurement.expression,
      type   = expression.type,
      start  = measurement.start,
      stop   = measurement.stop,
      filter = {t: {}},
      fields = {t: 1};

  // Copy any expression filters into the query object.
  expression.filter(filter);
  // Request any needed fields.
  expression.fields(fields);

  filter.t.$gte = start;
  filter.t.$lt = stop;

  db.events(type, function (error, collection) {
    if(error) return callback(error);
    collection.find(filter, fields, event_options, handleResponse);
  });

  function handleResponse(error, cursor){
    if (error) return callback(error);
    cursor.each(function(error, row) {
      if (error) return callback(error);
      if (row) callback(error, new Event(type, row.t, row.d, row._id));
      else callback();
    })
  }
}
Object.defineProperty(Event, "find", { value: find });

function save(db, callback){
  var self = this;
  if (this.validate) this.validate();

  db.events(self.type, function event_saver(error, collection){
    if (error) return callback(error);
    collection.save(self.to_wire(), function saver(error){
      callback(error, self);
    });
  });
};

// Validate the date and type.
function validate(){
  if (!type_re.test(this.type)) throw("invalid type");
  if (isNaN(this.time))         throw("invalid time");
};

function to_request(attrs){
  var ev = { time: this.time, data: this.data, type: this.type };
  if (this.id) ev.id = this.id;
  for (var key in attrs){ ev[key] = attrs[key]; }
  return ev;
};

function handle(error) {
  if (!error) return;
  metalog.error('event', error);
  throw error;
}

module.exports = Event;

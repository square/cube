'use strict';

var util = require('util'),
    _    = require('underscore');

function Model(){};

function modelize(ctor, super_) {
  super_ = super_ || Model;
  if(super_ !== ctor) util.inherits(ctor, super_);

  var properties = {};

  properties['eventize']      = { value: eventize };
  properties['_trace']        = { value: null, enumerable: false, writable: true, configurable: true };
  properties['setProperty']   = { value: function(name, desc){ Object.defineProperty(this, name, desc) }};
  properties['setProperties'] = {
    value: function(descs){
      var _this = this;
      _.keys(descs).forEach(function(key){
        Object.defineProperty(_this, key, descs[key]);
      })
    }
  };

  Object.defineProperties(ctor.prototype, properties);
  Object.defineProperty(ctor, 'setProperty', { value: function(name, desc){ Object.defineProperty(ctor.prototype, name, desc); }});
  Object.defineProperty(ctor, 'setProperties', {
    value: function(descs){
      var _this = this;
      _.keys(descs).forEach(function(key){
        Object.defineProperty(ctor.prototype, key, descs[key]);
      })
    }
  });

  return ctor;
};

function eventize() {
  var emitter = new (require('events').EventEmitter)(),
      _this   = this;

  Object.keys(Object.getPrototypeOf(emitter)).forEach(function(prop){
    Object.defineProperty(_this, prop, { value: function(args){
      emitter[prop].apply(emitter, arguments);
    }});
  });
}

modelize(Model);

Model.modelize = modelize;

module.exports = Model;
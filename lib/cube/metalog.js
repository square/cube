'use strict';

var util  = require("util"),
    _ = require("underscore");

var metalog = {
  putter:  null,
  log:     util.log,
  silent:  function(){ }
};

// adjust verboseness by reassigning `metalog.loggers.{level}`
// @example: quiet all logs
//   metalog.loggers.info = metalog.silent();
metalog.loggers = {
  info:  metalog.log,
  minor: metalog.log
};

// if true, cubify `metalog.event`s
metalog.send_events = true;

// --------------------------------------------------------------------------

// Cubify an event and (optionally) log it. The last parameter specifies the
// logger -- 'info' (the default), 'minor' or 'silent'.
metalog.event = function(name, hsh, logger){
  metalog[logger||"info"](name, hsh);
  if ((! metalog.send_events) || (! metalog.putter)) return;
  metalog.putter({ type: name, time: Date.now(), data: hsh });
};

// Always goes thru to metalog.log
metalog.warn = function(name, hsh){
  metalog.log(name + "\t" + (+Date.now()) + "\t" + JSON.stringify(hsh));
};

// Events important enough for the production log file. Does not cubify.
metalog.info = function(name, hsh){
  metalog.loggers.info(name + "\t" + JSON.stringify(hsh));
};

// Debug-level statements; loggers.minor is typically mapped to 'silent'.
metalog.minor = function(name, hsh){
  metalog.loggers.minor(name + "\t" + JSON.stringify(hsh));
};

// Dump the 'util.inspect' view of each argument to the console.
metalog.inspectify = function inspectify(args){
  for (var idx in arguments) {
    util.print(idx + ": ");
    var val = arguments[idx];
    if (_.isFunction(val)) val = val.toString().slice(0, 80);
    util.print(util.inspect(val, false, 2, true)+"\n"); // , null, true
  }
  util.print('----\n');
};

// wraps a callback, inspectifies its contents as it goes by
// @example
//   metalog.spy(callback, 'unary', this);
// @example you don't have to supply anything but the callback
//   metalog.spy(function(err, val){ ... })
metalog.spy = function spy(callback, name, ctxt){
  if (! name) name = 'spyable';
  if (! ctxt) ctxt = null;
  return function(){
    util.print(name+': ', callback, ' called with ', util.inspect(arguments), '\n');
    // metalog.inspectify(callback, arguments);
    return callback.apply(ctxt, arguments);
  };
};

module.exports = metalog;

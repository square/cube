'use strict';

var util  = require("util");

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
  minor: metalog.silent
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
    util.print(util.inspect(arguments[idx])+"\n");
  }
  util.print('----\n');
};

module.exports = metalog;

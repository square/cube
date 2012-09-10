'use strict';

// metalog -- log, trace and cubify internal processing stages
//
// * log progress to disk
// * trace a request through the process stack
// * 

var util  = require("util"),
    _ = require("underscore");

var metalog = {
  putter:  null,
  log:     util.log,
  silent:  function(){ }
};

var tracestack = {},     // container for traces
    tracecap   = 10000;  // max traces to track

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
metalog.event = function(label, hsh, logger){
  metalog[logger||"info"](label, hsh);
  metalog.cubify(label, hsh);
};

metalog.cubify = function(label, hsh){
  if ((! metalog.send_events) || (! metalog.putter)) return;
  hsh.at = label;
  metalog.putter({ type: 'cube', time: Date.now(), data: hsh });
}

metalog.logify = function logify(label, hsh, logger){
  hsh = hsh || {};
  try{
    logger(label + "\t" + JSON.stringify(hsh));
  } catch(error) {
    logger(label + "\t" + util.inspect(hsh));
  }
}

// --------------------------------------------------------------------------

// Always goes thru to metalog.log
metalog.warn = function(label, hsh){ metalog.logify(label, hsh, metalog.log); };

// Events important enough for the production log file. Does not cubify.
metalog.info = function(label, hsh){ metalog.logify(label, hsh, metalog.loggers.info); };

// Debug-level statements; loggers.minor is typically mapped to 'silent'.
metalog.minor = function(label, hsh){ metalog.logify(label, hsh, metalog.loggers.minor); };

// log an error; always goes thru to metalog.log
metalog.error = function(at, error, info){
  info = _.extend((info||{}), { at: at, error: error.message, stack: error.stack, code: error.status });
  metalog.logify('error', info, metalog.log);
};

// --------------------------------------------------------------------------

var trace_id = 1, boot = +(new Date());

var dump_keys = {tid: 4, beg: 15, boot: 4, 
                 tier: 8, start: 9, stop: 9, bin: 9,
                 mget: 4, find: 4, mf0: 4, mf1: 4, msb: 4,
                 mflt: 4, mfl0: 4, mfly: 4, msav: 4, mres: 4, resp: 4,
                 expr: 12};

function dump(hsh){
  var value = hsh.val;
  var fields = _.map(dump_keys, function(len, key){ return ((hsh[key] === undefined ? '' : hsh[key])+'          ').slice(0,len); });
  var extras = {}; for (var key in hsh){ if (!(key in dump_keys)) extras[key] = hsh[key]; }
  if (_.isNumber(value)) fields.push((value*100)/100);
  metalog.log(fields.join('|') + "\t" + (JSON.stringify(extras).slice(0,150)));
}  

function dump_header(){
  metalog.log(_.map(dump_keys, function(len, key){ return (key+'              ').slice(0,len); }).join('|'));
}  

var tracing = false;

if (tracing){

  metalog.trace = function(label, item, hsh){
    
    // try{ throw new Error("Trace"); } catch(e){ console.log(e.stack); }
    
    item = item || {}; hsh = hsh || {};
    var using = (hsh.using||{}); delete hsh.using;
    if (! item._trace) item._trace = { beg: +(new Date()), boot: ((new Date()) - boot)  };
    //
    item._trace = _.extend(item._trace, (using._trace||{}), hsh);
    //
    item._trace[label] = ((new Date()) - item._trace['beg']);
    if (! item._trace.tid) item._trace.tid = trace_id++;
    // if (value) item._trace['val'] = (Math.round(100*value)/100.0);
    // } catch(err){ metalog.log(err); }

    // metalog.warn('tr_'+label, item);
    // if (label == 'msbl') metalog.inspectify(label, item, using, hsh)

    return item;
  };

  metalog.dump_trace = function(label, item, hsh){
    var item = metalog.trace(label, item, hsh);
    var tr   = item._trace;
    function p(v,l){ var str = (v ? v.toString() : ''); return (v+'................').slice(0,l); }
    try{
      item._trace['end'] = +(new Date());
      if (Math.random() < 0.3) dump_header();
      dump(item._trace);
    } catch(err){ metalog.log(err); metalog.log(err.stack); }
    return item;
  };
} else {
  metalog.trace      = function(){};
  metalog.dump_trace = function(){};
}


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
metalog.spy = function spy(callback, label, ctxt){
  if (! label) label = 'spyable';
  if (! ctxt) ctxt = null;
  return function(){
    util.print(label+': ', callback, ' called with ', util.inspect(arguments), '\n');
    // metalog.inspectify(callback, arguments);
    return callback.apply(ctxt, arguments);
  };
};

module.exports = metalog;

'use strict';

var _ = require('underscore'),
    util    = require('util'),
    metalog = require('./metalog');

// milliseconds to sleep, if all jobs are satisfied, until checking again
var worker_sleep_ms = 100;

// ---- Broker ----

function Broker(name, interval){
  var workers = [];
  add_worker();
  this.name = name;  

  // `deferProxy(name, perform, *args, on_complete)` -- Issue a request with
  // controlled concurrency; send its results to all interested listeners.  We
  // use it so that when multiple clients are interested in a metric, we only
  // issue the query once, yet share the good news immediately.
  //
  // Worker will at some time invoke perform with the supplied args, tacking on
  // its own callback (`perform(*args, worker_cb)`, essentially). The task must,
  // success or failure, eventually invoke the proxy callback.
  //
  // When the task triggers the proxy callback with the result, every
  // `on_complete` handler waiting on that name is invoked with that result.
  // For the second and further tasks `defer`ing to a given mbox, the `perform`
  // function is ignored -- tasks with the same name must be interchangeable.
  //
  // @param [String]        name -- handle for the query
  // @param [Function]      perform(*args) -- function for worker to dispatch
  // @param [Array<Object>] *args -- args sent to `perform` when dispatched
  // @param [Function]      complete_cb -- called when the task completes
  //
  this.deferProxy = function deferProxy(){ //
    var args = _.toArray(arguments),
        name = args.shift(), perform = args.shift(), on_complete = args.pop();
    if (! (name && perform && on_complete)) throw new TypeError('you must supply a name, perform callback and on-complete handler: got ' + [name, perform, on_complete]);
    worker_for(name).add(name, perform, args, on_complete);
    return this;
  };

  function worker_for(name){
    return workers[0];
  }

  function add_worker(){
    var worker = new Worker((name+'-'+workers.length), interval);
    workers.push(worker);
    worker.start();
    return worker;
  }

  function stop(){ _.each(workers, function(worker){ worker.stop() })  };
  
  function report(){  return { workers: _.map(workers, function(worker){ return worker.report() }) }; }
  function toString(){ return util.inspect(this.report()); };

  Object.defineProperties(this, {
    stop:   {value: stop},
    report: {value: report}, toString: {value: toString},
  });
}

// ---- Worker ----

// A worker executes a set of tasks with parallelism 1
function Worker(qname, interval){
  var queue  = Object.create(null),
      active = null,
      self   = this,
      clock;
  this.qname = qname;

  function add(mbox, perform, args, on_complete) {
    var job = ((active && (active.mbox === mbox)) ? active : queue[mbox]);
    if (! job){ job = queue[mbox] = new Job(mbox, perform, args); }
    //
    job.listen(on_complete);
    metalog.minor('q_worker', {is: 'add', mbox: job.mbox, am: self.report(), job: job.report() });
    return job;
  }

  function invoke(job) {
    // move this job to be active
    active = job;
    delete queue[job.mbox];
    // add our callback as last arg. when triggered, it takes the arguments it
    // was triggered with and has job fire that at all its `on_complete`s, and
    // clears the active job (letting the next task start).
    job.args.push(function _completed(){
      var result = arguments;
      metalog.warn('q_worker', {is: '<-!', mbox: job.mbox, am: self.report(), result: util.inspect(result).slice(0,80) });
      if (job !== active) metalog.warn('q_worker', {is: 'ERR', am: qname, error: 'job was missing when callback triggered', self: self.report() });
      active = null;
      job.complete(result);
    });
    // start the task
    metalog.warn('q_worker', {is: '?->', mbox: job.mbox, am: self.report(), perform_args: util.inspect(job.args).slice(0,80) });
    try{ job.perform.apply(null, job.args); } catch(err){ metalog.warn('q_worker', {is: 'ERR', am: qname, as: 'performing job '+job.mbox, error: err.message }) };
  }

  function start(){
    if (clock){ return metalog.warn('q_worker', {is: 'ERR', am: self.report(), error: 'tried to start an already-running worker' }); }
    clock = setInterval(self.work, interval);
  }
  function stop(){
    metalog.info('q_worker', {is: 'stp'});
    if (! clock){ return metalog.warn('q_worker', {is: 'ERR', am: self.report(), error: 'tried to stop an already-stopped worker' }); }
    clearInterval(clock);
    clock = null;
  }

  function work (){
    var job;
    if (active)           { self.onWait();    }
    else if (job = next()){ self.invoke(job); }
    else                  { self.onIdle();    }
  };

  function size(){ return _.size(queue); }
  function next(){ return queue[ _.keys(queue).sort()[0] ]; }

  // function onIdle(){ util.print(' '+self.qname+'!'); };
  // function onWait(){ util.print(' '+self.qname+'@'); };
  function onIdle(){ };
  function onWait(){ };

  function report(){   return { qname: this.qname, size: this.size, queue: _.keys(this.queue) }; };
  function toString(){ return util.inspect(this.report()); };

  Object.defineProperties(this, {
    add:    {value: add},    size:     {get:   size},
    start:  {value: start},  stop:     {value: stop},
    report: {value: report}, toString: {value: toString},
    onWait: {value: onWait}, onIdle:   {value: onIdle}
  });
}

// ---- Job ----

function Job(mbox, perform, args){
  _.extend(this, { mbox: mbox, perform: perform, args: args, on_completes: [] });
}
Job.prototype.complete = function(result){
  for (var ii in this.on_completes){
    process.nextTick(function(){ this.on_completes[ii].apply(null, result); });
  };
}
Job.prototype.listen   = function(on_complete){
  this.on_completes.push(on_complete);
};
Job.prototype.toString = function (){ return util.inspect(this.report()); };
Job.prototype.report   = function (){ return this; }

// ----

module.exports = { Broker: Broker, Job: Job, Worker: Worker };

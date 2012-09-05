'use strict';

var _ = require('underscore'),
    util        = require("util"),
    vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    broker      = require("../lib/cube/broker"),
                  Job = broker.Job, Broker = broker.Broker, Worker = broker.Worker,
    metalog = require('../lib/cube/metalog');

var suite = vows.describe("broker");

var squarer = function(ii, cb){ cb(null, ii*ii, 'squarer'); };

assert.isCalledTimes = function(ctxt, reps){
  var results = [];
  setTimeout(function(){ ctxt.callback(new Error('timeout: need '+reps+' results only have '+util.inspect(results))); }, 2000);
  return function _is_called_checker(){
    results.push(_.toArray(arguments));
    if (results.length >= reps) ctxt.callback(null, results);
  };
}
assert.isNotCalled = function(name){
  return function(){ throw new Error(name + ' should not have been called, but was'); };
};

function example_worker(paused){
  var worker = new Worker('worker', 50);
  // worker.idle = _.identity;
  if (! paused) worker.start();
  return worker;
}


function example_job(){ return (new Job('smurf', squarer, [7])); };

suite.addBatch({
  // 'Worker': {
  //   topic: example_worker,
  //   // '.new': {
  //   //   '': function(worker){
  //   //     test_helper.inspectify('new worker', worker, worker);
  //   //     worker.add('smurfette', squarer, [3], metalog.inspectify);
  //   //   },
  //   // },
  //   '.invoke': {
  //     topic: function(worker){
  //       var ctxt = this;
  //       ctxt.checker      = assert.isCalledTimes(ctxt, 3);
  //       ctxt.performances = 0;
  //       // shortly, worker will invoke perfom (once). 200 ms later, `perform`
  //       // will call `worker`'s proxy callback, which invokes all 3 callbacks.
  //       var perform = function(cb){ ctxt.performances++; setTimeout(function(){cb('hi')}, 200); };
  //       worker.add('thrice', perform, [], ctxt.checker);
  //       worker.add('thrice', assert.isNotCalled('perform'), [], ctxt.checker);
  //       worker.add('thrice', assert.isNotCalled('perform'), [], ctxt.checker);
  //       this.worker = worker;
  //     },
  //     'calls perform exactly once':     function(){ assert.equal(this.performances, 1); },
  //     'calls all registered callbacks': function(results){ assert.deepEqual(results, [['hi'], ['hi'], ['hi']]) },
  //     teardown: function(){
  //       this.worker.stop();
  //     }      
  //   }
  // },
  
  // 'Job': {
  //   '.new': {
  //     topic: example_job,
  //     '': function(job){
  //       assert.deepEqual(job, {name: 'smurf', perform: squarer, args: [7], on_completes: []});
  //     },
  //   },
  //   '.listen': {
  //     topic: function(){
  //       var job = example_job();
  //       job.listen(squarer);
  //       return job;
  //     },
  //     '': function(job){
  //       test_helper.inspectify(job, job.toString());
  //     }
  //   },
  //   // '.add': {
  //   //   topic: example_job,
  //   //   '': function(job){
  //   //   }
  //   // },
  // },
  
  'Broker': {
    'handles interleaved jobs': {
      topic: function(){
        var ctxt    = this,
            broker  = this.broker = new Broker('test', 10),
            ignored = assert.isNotCalled('perform');
        ctxt.perfs   = {a: 0, b: 0, c:0};
        ctxt.checker = assert.isCalledTimes(ctxt, 8);
        var task_a = function(ii, a2, cb){ ctxt.perfs.a++; setTimeout(function(){cb('result_a', ii*ii, a2)}, 10); };
        var task_b = function(ii,     cb){ ctxt.perfs.b++; setTimeout(function(){cb('result_b', ii*ii    )}, 20); };
        var task_c = function(ii,     cb){ ctxt.perfs.c++; setTimeout(function(){cb('result_c', ii*ii    )}, 300); };
        // will go second: jobs are sorted
        broker.deferProxy('task_b',   task_b,  1,       ctxt.checker);
        broker.deferProxy('task_b',   ignored, '<>',    ctxt.checker);
        // will go first
        broker.deferProxy('task_a',   task_a,  0,  '?', ctxt.checker);
        broker.deferProxy('task_a',   ignored, '<>',    ctxt.checker);
        // will go third
        broker.deferProxy('task_c',   task_c,  2,       ctxt.checker);
        // a & b will be done; c (takes 300ms) will still be running.
        setTimeout(function(){
          broker.deferProxy('task_a', task_a,  3,  '!', ctxt.checker);
          broker.deferProxy('task_c', ignored, '<>',    ctxt.checker);
          broker.deferProxy('task_a', task_a,  3,  '!', ctxt.checker);
        }, 200);
      },
      'calls perform exactly once':     function(){ assert.deepEqual(this.perfs, {a: 2, b: 1, c: 1}); },
      'calls all registered callbacks': function(results){
        assert.deepEqual(results, [
          ['result_a', 0, '?'], ['result_a', 0, '?'],
          ['result_b', 1],      ['result_b', 1],
          ['result_c', 4],      ['result_c', 4],
          ['result_a', 9, '!'], ['result_a', 9, '!']
        ])
      },
      teardown: function(){
        this.broker.stop();
      }
    }
  },
});
      


suite['export'](module);

'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require('./test_helper');

var suite = vows.describe("metalog");

suite.with_log = function(batch){
  suite.addBatch({
    '':{
      topic: function(){
        var metalog = require("../lib/cube/metalog");
        var logged = this.logged = { infoed: [], minored: [], putted: [] };
        this.original = { send_events: metalog.send_events, putter: metalog.putter, info: metalog.loggers.info, minor: metalog.loggers.minor };
        metalog.send_events   = true;
        metalog.loggers.info  = function(line){ logged.infoed.push(line);  };
        metalog.loggers.minor = function(line){ logged.minored.push(line); };
        metalog.putter        = function(line){ logged.putted.push(line);  };
        return metalog;
      },
      metalog: batch,
      teardown: function(metalog){
        metalog.loggers.info  = this.original.info;
        metalog.loggers.minor = this.original.minor;
        metalog.putter        = this.original.putter;
      }
    }
  });
  return suite;
};

suite.with_log({
  '.info': {
    'logs record to metalog.logers.info': function(metalog){
      metalog.info('reactor_level', { criticality: 7, cores: 'leaking' });
      assert.equal(this.logged.infoed.pop(), 'reactor_level\t{"criticality":7,"cores":"leaking"}');
      assert.deepEqual(this.logged, { infoed: [], minored: [], putted: [] });
    }
  }
}).with_log({
  '.minor': {
    'logs record to metalog.loggers.minor': function(metalog){
      metalog.minor('reactor_level', { modacity: 3 });
      assert.equal(this.logged.minored.pop(), 'reactor_level\t{"modacity":3}');
      assert.deepEqual(this.logged, { infoed: [], minored: [], putted: [] });
    }
  }
}).with_log({
  '.event': {
    'with send_events=true': {
      topic: function(metalog){
        metalog.send_events = true;
        metalog.event('reactor_level', { criticality: 9, hemiconducers: 'relucting' });
        return metalog;
      },
      'logs record to metalog.info by default': function(metalog){
        assert.equal(this.logged.infoed.pop(), 'reactor_level\t{"criticality":9,"hemiconducers":"relucting"}');
      },
      'writes an event to cube itself': function(metalog){
        var event = this.logged.putted.pop();
        event.time = 'whatever';
        assert.deepEqual(event, {
          data: { hemiconducers: 'relucting', criticality: 9 },
          type: 'reactor_level',
          time: 'whatever'
        });
      }
    }
  }
}).with_log({
  '.event': {
    'with send_events=false': {
      topic: function(metalog){
        metalog.send_events = false;
        metalog.event('reactor_level', { criticality: 10, hemiconducers: 'fremulating' });
        return metalog;
      },
      'logs record to metalog.loggers.info': function(metalog){
        assert.equal(this.logged.infoed.pop(), 'reactor_level\t{"criticality":10,"hemiconducers":"fremulating"}');
      },
      'does not write an event to cube': function(metalog){
        assert.deepEqual(this.logged, { infoed: [], minored: [], putted: [] });
      }
    }
  }
}).with_log({
  '.event': {
    'last parameter overrides log target': {
      topic: function(metalog) {
        metalog.send_events = true;
        metalog.event('reactor_level', { criticality: 3, hemiconducers: 'cromulent' }, 'minor');
        metalog.event('reactor_level', { criticality: 2, hemiconducers: 'whispery'  }, 'silent');
        return metalog;
      },
      '': function(metalog){
        assert.equal(this.logged.minored.pop(), 'reactor_level\t{"criticality":3,"hemiconducers":"cromulent"}');
        assert.equal(this.logged.putted.pop().data.criticality, 2);
        assert.equal(this.logged.putted.pop().data.criticality, 3);
        assert.deepEqual(this.logged, { infoed: [], minored: [], putted: [] });
      }
    }
  }
});

function dummy_logger(arg){}

suite.addBatch({
  'metalog':{
    topic: function(){ return require("../lib/cube/metalog"); },
    '': {
      'loggers persist across factory invocation': function(metalog){
        metalog.orig_minor    = metalog.loggers.minor;
        metalog.loggers.minor = dummy_logger;
        var ml2 = require("../lib/cube/metalog");
        assert.deepEqual(metalog, ml2);
        assert.deepEqual(metalog.loggers.minor,    dummy_logger);
        assert.notDeepEqual(metalog.loggers.minor, metalog.orig_minor);
      },
      teardown: function(metalog){ metalog.loggers.minor = metalog.orig_minor; }
    }
  }});

suite['export'](module);

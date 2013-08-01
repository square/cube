'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    Event       = require("../lib/cube/models/event"),
    event       = require("../lib/cube/event"),
    config      = require('../config/cube');

var suite = vows.describe("event");

var ice_cubes_good_day = Date.UTC(1992, 1, 20, 1,  8,  7),
    fuck_wit_dre_day   = Date.UTC(1993, 2, 18, 8, 44, 54);

suite.addBatch(test_helper.batch({
  topic: function(test_db) {
    return this.putter = event.putter(test_db);
  },
  'invalidates': {
    topic: function(putter){
      var _this = this;
      putter((new Event('test', ice_cubes_good_day, {value: 3})).to_request(), function(){
        putter((new Event('test', fuck_wit_dre_day, {value: 3})).to_request(), _this.callback);});
    },
    'correct tiers': function(a,b){
      var ts = this.putter.invalidator().tsets;
      assert.deepEqual(ts, { 'test': {
        10e3:    [new Date('1992-02-20T01:08:00Z'), new Date('1993-03-18T08:44:50Z') ],
        60e3:    [new Date('1992-02-20T01:08:00Z'), new Date('1993-03-18T08:44:00Z') ],
        300e3:   [new Date('1992-02-20T01:05:00Z'), new Date('1993-03-18T08:40:00Z') ],
        3600e3:  [new Date('1992-02-20T01:00:00Z'), new Date('1993-03-18T08:00:00Z') ],
        86400e3: [new Date('1992-02-20T00:00:00Z'), new Date('1993-03-18T00:00:00Z') ]
      }});
    }
  },
  'callback': {
    topic: function(putter){
      putter((new Event('test', fuck_wit_dre_day, {value: 3})).to_request(), this.callback);
    },
    'no error arg': function(arg1, arg2){
      assert.instanceOf(arg1, Event);
      assert.typeOf(arg2, 'undefined');
    }
  },
  teardown: function(putter){ putter.stop(this.callback); }
}));

suite.addBatch(test_helper.batch({
  topic: function(test_db) {
    var horizon = new Date() - fuck_wit_dre_day + (1000 * 60);

    // Temporarily override horizons settings
    this.oldHorizons = config.get('horizons');
    config.set('horizons', {
      invalidation: horizon
    });
    return event.putter(test_db.db);
  },
  'events past invalidation horizon': {
    topic: function(putter){
      var _this    = this,
          event    = new Event('test', ice_cubes_good_day, {value: 3});
      this.ret = putter(event.to_request(), this.callback);
    },
    'should error': function(error, response){
      assert.deepEqual(error, {error: "event before invalidation horizon"});
    },
    'should return -1': function(error, response){
      assert.equal(this.ret, -1);
    }
  }/*,
  teardown: function() {
    config.set('horizons', this.oldHorizons);
  }*/
}));

suite['export'](module);

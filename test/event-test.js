'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    models      = require("../lib/cube/models"), units = models.units, Event = models.Event,
    event       = require("../lib/cube/event");

var suite = vows.describe("event");

var ice_cubes_good_day = Date.UTC(1992, 1, 20, 1, 8,  7),
    fuck_wit_dre_day   = Date.UTC(1993, 2, 18, 8,44, 54)


suite.addBatch(test_helper.batch({
  topic: function(test_db) {
    return event.putter(test_db.db);
  },
  'invalidates': {
    topic: function(putter){
      var ctxt = this;
      putter((new Event('test', ice_cubes_good_day, {value: 3})).to_request(), function(){
        putter((new Event('test', fuck_wit_dre_day,   {value: 3})).to_request(), ctxt.callback) });
    },
    'heckya': function(){
      var ts = event.invalidator().tsets();
      assert.deepEqual(ts, { 'test': {
        10e3:    [new Date('1992-02-20T01:08:00Z'), new Date('1993-03-18T08:44:50Z') ],
        60e3:    [new Date('1992-02-20T01:08:00Z'), new Date('1993-03-18T08:44:00Z') ],
        300e3:   [new Date('1992-02-20T01:05:00Z'), new Date('1993-03-18T08:40:00Z') ],
        3600e3:  [new Date('1992-02-20T01:00:00Z'), new Date('1993-03-18T08:00:00Z') ],
        86400e3: [new Date('1992-02-20T00:00:00Z'), new Date('1993-03-18T00:00:00Z') ]
      }});
    }
  }  
}));

suite.export(module);

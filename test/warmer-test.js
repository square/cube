'use strict';

var _           = require('underscore'),
    vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    event       = require("../lib/cube/event"),
    config      = require("../config/cube"),
    warmer      = require("../lib/cube/warmer");

var suite = vows.describe("warmer");

suite.addBatch(test_helper.batch({
  topic: function(test_db) {
    // Temporarily override horizons settings
    this.oldHorizons = config.get('horizons');
    config.set('horizons', {
      calculation: 30000
    });

    var board  = { pieces: [{ query: "sum(test(value))" }] },
        nowish = this.nowish = (10e3 * Math.floor(new Date()/10e3)),
        putter = this.putter = event.putter(test_db),
        _this  = this;

    this.warmer = warmer();

    putter({type: 'test', time: nowish + 500, data: {value: 10}}, function(){
      putter({type: 'test', time: nowish + 2000, data: {value: 5}}, function(){
        test_db.using_objects("boards", [board], {callback: function(error){
          if(error) return _this.callback(error);
          _this.callback(null, test_db);
        }});
      });
    });
  },
  'calculates': {
    topic: function(test_db){
      var _this = this;

      this.warmer.start();
      setTimeout(function(){
        test_db.metrics('test', function(error, collection){
          collection.find().toArray(_this.callback);
        });
      }, 1000);
    },
    'correct number of metrics':  function(metrics){ assert.equal(metrics.length, 3); },
    'correct values for metrics': function(metrics){
      assert.equal(metrics[0].v, 0);  assert.equal(+metrics[0]._id.t, +this.nowish - 20000);
      assert.equal(metrics[1].v, 0);  assert.equal(+metrics[1]._id.t, +this.nowish - 10000);
      assert.equal(metrics[2].v, 15); assert.equal(+metrics[2]._id.t, +this.nowish);
    }
  },
  teardown: function(){
    config.set('horizons', this.oldHorizons);
    this.warmer.stop();
    this.putter.stop(this.callback);
  }
}));

suite['export'](module);

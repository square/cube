'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    models      = require("../lib/cube/models"), units = models.units,
    event       = require("../lib/cube/event"),
    metric      = require("../lib/cube/metric");

// as a hack to get updates to settle, we need to insert delays.
// if you see heisen-errors in the metrics tests, increase these.
var step_testing_delay  = 250,
    batch_testing_delay = 500;

var suite = vows.describe("metric");

var nowish = Date.now(), nowish10 = (10e3 * Math.floor(nowish/10e3));
var steps = {
  10e3:    function(date, n) { return new Date((Math.floor(date / units.second10) + n) * units.second10); },
  60e3:    function(date, n) { return new Date((Math.floor(date / units.minute)   + n) * units.minute); },
  300e3:   function(date, n) { return new Date((Math.floor(date / units.minute5)  + n) * units.minute5); },
  3600e3:  function(date, n) { return new Date((Math.floor(date / units.hour)     + n) * units.hour); },
  86400e3: function(date, n) { return new Date((Math.floor(date / units.day)      + n) * units.day); }
};

steps[units.second10].description = "10-second";
steps[units.minute  ].description = "1-minute";
steps[units.minute5 ].description = "5-minute";
steps[units.hour    ].description = "1-hour";
steps[units.day     ].description = "1-day";

function gen_request(attrs){
  var req = { start: nowish, stop: nowish, step: units.second10, expression: 'sum(test)'};
  for (var key in attrs){ req[key] = attrs[key]; }
  return req;
}

function assert_invalid_request(req, expected_err) {
  return {
    topic:   function(getter){ this.ret = getter(gen_request(req), this.callback); },
    'fails':      function(err, val){ assert.deepEqual(err, expected_err); },
    'returns -1': function(err, val){ assert.equal(this.ret, -1); }
  };
}

suite.addBatch(test_helper.batch({
  topic: function(test_db) {
    return metric.getter(test_db.db);
  },
  'invalid start': assert_invalid_request({start: 'THEN'}, {error: "invalid start"}),
  'invalid stop':  assert_invalid_request({stop:  'NOW'},  {error: "invalid stop"}),
  'invalid step':  assert_invalid_request({step:  'LEFT'}, {error: "invalid step"}),
  'invalid expression': assert_invalid_request({expression: 'DANCE'}, {error: "invalid expression"}),

  'with request id' : {
    topic:   function(getter){ this.ret = getter(gen_request({id: 'joe', expression: 'sum(test(1))'}), this.callback); },
    'fires callback with id': function(result, j_){
      assert.equal(result.id, 'joe');
      assert.equal(result.value, (result.time > nowish10 ? undefined : 0));
      assert.include([nowish10, 10000+nowish10], +result.time);
    }
  }

})).addBatch(test_helper.batch({
  topic: function(test_db) {
    return metric.getter(test_db.db);
  },
  'no request id' : {
    topic:   function(getter){ this.ret = getter(gen_request({}), this.callback); },
    'fires callback with no id': function(result, j_){
      assert.isFalse("id" in result);
      assert.equal(result.value, (result.time > nowish10 ? undefined : 0));
      assert.include([nowish10, 10000+nowish10], +result.time);
    }
  }

}));

suite.addBatch(test_helper.batch({
  topic: function(test_db) {
    var putter = event.putter(test_db.db),
    getter = metric.getter(test_db.db),
    callback = this.callback;

    // Seed the events table with a simple event: a value going from 0 to 2499
    for (var i = 0; i < 2500; i++) {
      putter({
        type: "test",
        time: new Date(Date.UTC(2011, 6, 18, 0, Math.sqrt(i) - 10)).toISOString(),
        data: {i: i}
      });
    }

    // So the events can settle in, wait `batch_testing_delay` ms before continuing
    setTimeout(function() { callback(null, getter); }, batch_testing_delay);
  },

  "unary expression": metricTest({
    expression: "sum(test)",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    60e3:    [0, 0, 0, 1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 39, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87, 89, 91, 93, 95, 97, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    300e3:   [0, 17, 65, 143, 175, 225, 275, 325, 375, 425, 475, 0, 0],
    3600e3:  [82, 2418],
    86400e3: [82, 2418]
  }),

  "unary expression with data accessor": metricTest({
    expression: "sum(test(i))",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    300e3:   [0, 136, 3185, 21879, 54600, 115200, 209550, 345150, 529500, 770100, 1074450, 0, 0],
    3600e3:  [3321, 3120429],
    86400e3: [3321, 3120429]
  }),

  "unary expression with compound data accessor": metricTest({
    expression: "sum(test(i / 100))",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    300e3:   [0, 1.36, 31.85, 218.79, 546, 1152, 2095.5, 3451.5, 5295, 7701, 10744.5, 0, 0],
    3600e3:  [33.21, 31204.29],
    86400e3: [33.21, 31204.29]
  }),

  "compound expression (sometimes fails due to race condition?)": metricTest({
    expression: "max(test(i)) - min(test(i))",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    300e3:   [NaN, 16, 64, 142, 174, 224, 274, 324, 374, 424, 474, NaN, NaN],
    3600e3:  [81, 2417],
    86400e3: [81, 2417]
  }),

  "non-pyramidal expression": metricTest({
    expression: "distinct(test(i))",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    300e3:   [0, 17, 65, 143, 175, 225, 275, 325, 375, 425, 475, 0, 0],
    3600e3:  [82, 2418],
    86400e3: [82, 2418]
  }),

  "compound pyramidal and non-pyramidal expression": metricTest({
    expression: "sum(test(i)) - median(test(i))",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    300e3:   [NaN, 128, 3136, 21726, 54288, 114688, 208788, 344088, 528088, 768288, 1072188, NaN, NaN],
    3600e3:  [3280.5, 3119138.5],
    86400e3: [3280.5, 3119138.5]
  }),

  "compound with constant expression": metricTest({
    expression: "-1 + sum(test)",
    start:      "2011-07-17T23:47:00.000Z",
    stop:       "2011-07-18T00:50:00.000Z"
  }, {
    300e3:   [-1, 16, 64, 142, 174, 224, 274, 324, 374, 424, 474, -1, -1],
    3600e3:  [81, 2417],
    86400e3: [81, 2417]
  })

}));

// metricTest -- generates test tree for metrics.
//
// Gets the metric, checks it was calculated correctly from events seeded above;
// then does it again (on a delay) to check that it was cached.
//
// @example given `{ 'unary expression': metricTest({..}, { 60_000: [0, 0, ...], 86_400_000: [82, 2418] })`
//
//    { 'unary expression': {
//        'at 1-minute intervals': {
//          topic:       function get_metrics_with_delay(getter){},
//          'sum(test)': function metrics_assertions(actual){},
//          '(cached)': {
//            topic:       function get_metrics_with_delay(err, getter){},
//            'sum(test)': function metrics_assertions(actual){} } },
//        'at 1-day intervals': {
//          topic:       function get_metrics_with_delay(getter){},
//          'sum(test)': function metrics_assertions(actual){},
//          '(cached)': {
//            topic:       function get_metrics_with_delay(err, getter){},
//            'sum(test)': function metrics_assertions(actual){} } }
//      }
//    }
//
function metricTest(request, expected) {
  // { 'at 1-minute intervals': { }, 'at 1-day intervals': { } }
  var tree = {}, k;
  for (var step in expected) tree["at " + steps[step].description + " intervals"] = testStep(step, expected[step]);
  return tree;

  //
  // {
  //   topic: get_metrics_with_delay,
  //   expression: function(){
  //     // rounds down the start time (inclusive)
  //     // formats UTC time in ISO 8601
  //     ...
  //     // returns the expected values
  //   },
  //   '(cached)': {
  //     topic: get_metrics_with_delay,
  //     expression: function(){
  //       // rounds down the start time (inclusive)
  //       ...
  //     }
  //   }
  // }
  //
  function testStep(step, expected) {
    var start = new Date(request.start),
        stop  = new Date(request.stop);

    var subtree = {
      topic:      get_metrics_with_delay(0),
      '(cached)': { topic: get_metrics_with_delay(1) }
    };
    subtree[request.expression] = metrics_assertions();
    subtree["(cached)"][request.expression] = metrics_assertions();
    return subtree;

    function get_metrics_with_delay(depth){ return function(){
      var actual   = [],
          timeout  = setTimeout(function() { cb("Time's up!"); }, 10000),
          cb       = this.callback,
          req      = Object.create(request),
          getter   = arguments[depth];
      req.step = step;
      // Wait long enough for the events to have settled in the db.  The
      // non-cached (depth=0) round can all start in parallel, making this an
      // effective `nextTick`. On the secon
      setTimeout(function() {
        // ... then invoke the metrics getter. As responses roll in, push them
        // on to 'actual'; we're done when the 'stop' time is hit
        getter(req, function(response) {
          if (response.time >= stop) {
            clearTimeout(timeout);
            cb(null, actual.sort(function(a, b) { return a.time - b.time; }));
          } else {
            actual.push(response);
          }
        });
      }, depth * step_testing_delay);
    };}

    function metrics_assertions(){ return {
      'rounds down the start time (inclusive)': function(actual) {
        var floor = steps[step](start, 0);
        assert.deepEqual(actual[0].time, floor);
      },

      'rounds up the stop time (exclusive)': function(actual){
        var ceil = steps[step](stop, 0);
        if (!(ceil - stop)) ceil = steps[step](stop, -1);
        assert.deepEqual(actual[actual.length - 1].time, ceil);
      },

      'formats UTC time in ISO 8601': function(actual){
        actual.forEach(function(d) {
          assert.instanceOf(d.time, Date);
          assert.match(JSON.stringify(d.time), /[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:00.000Z/);
        });
      },

      'returns exactly one value per time': function(actual){
        var i = 0, n = actual.length, t = actual[0].time;
        while (++i < n) assert.isTrue(t < (t = actual[i].time));
      },

      'each metric defines only time and value properties': function(actual){
        actual.forEach(function(d) {
          assert.deepEqual(Object.keys(d), ["time", "value"]);
        });
      },

      'returns the expected times': function(actual){
        var floor = steps[step],
        time = floor(start, 0),
        times = [];
        while (time < stop) {
          times.push(time);
          time = floor(time, 1);
        }
        assert.deepEqual(actual.map(function(d) { return d.time; }), times);
      },

      'returns the expected values': function(actual){
        var actualValues = actual.map(function(d) { return d.value; });
        assert.equal(expected.length, actual.length, "expected " + expected + ", got " + actualValues);
        expected.forEach(function(value, i) {
          if (Math.abs(actual[i].value - value) > 1e-6) {
            assert.fail(actual.map(function(d) { return d.value; }), expected, "expected {expected}, got {actual} at " + actual[i].time.toISOString());
          }
        });
      }

    };} // metric assertions
  } // subtree
} // tree

suite.export(module);

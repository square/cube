var vows = require("vows"),
    assert = require("assert"),
    test = require("./helpers"),
    event = require("../lib/cube/event"),
    metric = require("../lib/cube/metric");

var suite = vows.describe("metric");

var steps = {
  1e4: function(date, n) { return new Date((Math.floor(date / 1e4) + n) * 1e4); },
  6e4: function(date, n) { return new Date((Math.floor(date / 6e4) + n) * 6e4); },
  3e5: function(date, n) { return new Date((Math.floor(date / 3e5) + n) * 3e5); },
  36e5: function(date, n) { return new Date((Math.floor(date / 36e5) + n) * 36e5); },
  864e5: function(date, n) { return new Date((Math.floor(date / 864e5) + n) * 864e5); }
};

steps[1e4].description = "10-second";
steps[6e4].description = "1-minute";
steps[3e5].description = "5-minute";
steps[36e5].description = "1-hour";
steps[864e5].description = "1-day";

suite.addBatch(test.batch({
  topic: function(test) {
    var putter = event.putter(test.db),
        getter = metric.getter(test.db),
        callback = this.callback;

    for (var i = 0; i < 2500; i++) {
      putter({
        type: "test",
        time: new Date(Date.UTC(2011, 6, 18, 0, Math.sqrt(i) - 10)).toISOString(),
        data: {i: i}
      });
    }

    function waitForEvents() {
      test.db.collection("test_events").count(function(err,count) {
        if (count == 2500) {
          callback(null, getter);
        } else {
          setTimeout(waitForEvents, 10);
        }
      });
    }

    setTimeout(waitForEvents,10);
  },

  "unary expression": metricTest({
      expression: "sum(test)",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      6e4: [0, 0, 0, 1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 39, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87, 89, 91, 93, 95, 97, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
      3e5: [0, 17, 65, 143, 175, 225, 275, 325, 375, 425, 475, 0, 0],
      36e5: [82, 2418],
      864e5: [82, 2418]
    }
  ),

  "unary expression with data accessor": metricTest({
      expression: "sum(test(i))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z"
    }, {
      3e5: [0, 136, 3185, 21879, 54600, 115200, 209550, 345150, 529500, 770100, 1074450, 0, 0],
      36e5: [3321, 3120429],
      864e5: [3321, 3120429]
    }
  ),

  "unary expression with compound data accessor": metricTest({
      expression: "sum(test(i / 100))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z"
    }, {
      3e5: [0, 1.36, 31.85, 218.79, 546, 1152, 2095.5, 3451.5, 5295, 7701, 10744.5, 0, 0],
      36e5: [33.21, 31204.29],
      864e5: [33.21, 31204.29]
    }
  ),

  "max expression": metricTest({
      expression: "max(test(i))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      36e5: [81, 2499],
      864e5: [81, 2499]
    }
  ),

  "min expression": metricTest({
      expression: "min(test(i))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      36e5: [0, 82],
      864e5: [0, 82]
    }
  ),

  "compound expression": metricTest({
      expression: "max(test(i)) - min(test(i))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      3e5: [NaN, 16, 64, 142, 174, 224, 274, 324, 374, 424, 474, NaN, NaN],
      36e5: [81, 2417],
      864e5: [81, 2417]
    }
  ),

  "non-pyramidal expression": metricTest({
      expression: "distinct(test(i))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      3e5: [0, 17, 65, 143, 175, 225, 275, 325, 375, 425, 475, 0, 0],
      36e5: [82, 2418],
      864e5: [82, 2418]
    }
  ),

  "compound pyramidal and non-pyramidal expression": metricTest({
      expression: "sum(test(i)) - median(test(i))",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      3e5: [NaN, 128, 3136, 21726, 54288, 114688, 208788, 344088, 528088, 768288, 1072188, NaN, NaN],
      36e5: [3280.5, 3119138.5],
      864e5: [3280.5, 3119138.5]
    }
  ),

  "compound with constant expression": metricTest({
      expression: "-1 + sum(test)",
      start: "2011-07-17T23:47:00.000Z",
      stop: "2011-07-18T00:50:00.000Z",
    }, {
      3e5: [-1, 16, 64, 142, 174, 224, 274, 324, 374, 424, 474, -1, -1],
      36e5: [81, 2417],
      864e5: [81, 2417]
    }
  )
}));

suite.export(module);

function metricTest(request, expected) {
  var t = {}, k;
  for (k in expected) t["at " + steps[k].description + " intervals"] = testStep(k, expected[k]);
  return t;

  function testStep(step, expected) {
    var t = testStepDepth(0, step, expected);
    t["(cached)"] = testStepDepth(1, step, expected);
    return t;
  }

  function testStepDepth(depth, step, expected) {
    var start = new Date(request.start),
        stop = new Date(request.stop);

    var test = {
      topic: function() {
        var actual = [],
            timeout = setTimeout(function() { cb("Time's up!"); }, 10000),
            cb = this.callback,
            req = Object.create(request),
            test = arguments[depth];
        req.step = step;
        setTimeout(function() {
          test(req, function(response) {
            if (response.time >= stop) {
              clearTimeout(timeout);
              cb(null, actual.sort(function(a, b) { return a.time - b.time; }));
            } else {
              actual.push(response);
            }
          });
        }, depth * 250);
      }
    };

    test[request.expression] = function(actual) {

      // rounds down the start time (inclusive)
      var floor = steps[step](start, 0);
      assert.deepEqual(actual[0].time, floor);

      // rounds up the stop time (exclusive)
      var ceil = steps[step](stop, 0);
      if (!(ceil - stop)) ceil = steps[step](stop, -1);
      assert.deepEqual(actual[actual.length - 1].time, ceil);

      // formats UTC time in ISO 8601
      actual.forEach(function(d) {
        assert.instanceOf(d.time, Date);
        assert.match(JSON.stringify(d.time), /[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:00.000Z/);
      });

      // returns exactly one value per time
      var i = 0, n = actual.length, t = actual[0].time;
      while (++i < n) assert.isTrue(t < (t = actual[i].time));

      // each metric defines only time and value properties
      actual.forEach(function(d) {
        assert.deepEqual(Object.keys(d), ["time", "value"]);
      });

      // returns the expected times
      var floor = steps[step],
          time = floor(start, 0),
          times = [];
      while (time < stop) {
        times.push(time);
        time = floor(time, 1);
      }
      assert.deepEqual(actual.map(function(d) { return d.time; }), times);

      // returns the expected values
      var actualValues = actual.map(function(d) { return d.value; });
      assert.equal(expected.length, actual.length, "expected " + expected + ", got " + actualValues);
      expected.forEach(function(value, i) {
        if (Math.abs(actual[i].value - value) > 1e-6) {
          assert.fail(actual.map(function(d) { return d.value; }), expected, "expected {expected}, got {actual} at " + actual[i].time.toISOString());
        }
      });

    };

    return test;
  }
}

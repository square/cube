var vows = require("vows"),
    assert = require("assert"),
    reduces = require("../lib/cube/reduces");

var suite = vows.describe("reduces");

suite.addBatch({
  "reduces": {
    "contains exactly the expected reduces": function() {
      var keys = [];
      for (var key in reduces) {
        keys.push(key);
      }
      keys.sort();
      assert.deepEqual(keys, ["distinct", "max", "median", "min", "sum"]);
    }
  },

  "distinct": {
    topic: function() {
      return reduces.distinct;
    },
    "empty is zero": function(reduce) {
      assert.strictEqual(reduce.empty, 0);
    },
    "is not pyramidal": function(reduce) {
      assert.isTrue(!reduce.pyramidal);
    },
    "returns the number of distinct values": function(reduce) {
      assert.equal(reduce([1, 2, 3, 2, 1]), 3);
    },
    "determines uniqueness based on string coercion": function(reduce) {
      assert.equal(reduce([{}, {}, {}]), 1);
      assert.equal(reduce([{}, "[object Object]", new Object]), 1);
      assert.equal(reduce([new Number(1), 1, "1"]), 1);
      assert.equal(reduce([new Number(1), 2, "3", "2", 3, new Number(1)]), 3);
      assert.equal(reduce([{toString: function() { return 1; }}, 1, 2]), 2);
    }
  },

  "max": {
    topic: function() {
      return reduces.max;
    },
    "empty is undefined": function(reduce) {
      assert.strictEqual(reduce.empty, undefined);
    },
    "is pyramidal": function(reduce) {
      assert.isTrue(reduce.pyramidal);
    },
    "returns the maximum value": function(reduce) {
      assert.equal(reduce([1, 2, 3, 2, 1]), 3);
    },
    "ignores undefined and NaN": function(reduce) {
      assert.equal(reduce([1, NaN, 3, undefined, null]), 3);
    },
    "compares using natural order": function(reduce) {
      assert.equal(reduce([2, 10, 3]), 10);
      assert.equal(reduce(["2", "10", "3"]), "3");
      assert.equal(reduce(["2", "10", 3]), 3); // "2" < "10", "10" < 3
      assert.equal(reduce([3, "2", "10"]), "10"); // "2" < 3, 3 < "10"
    },
    "returns the first of equal values": function(reduce) {
      assert.strictEqual(reduce([1, new Number(1)]), 1);
    }
  },

  "median": {
    topic: function() {
      return reduces.median;
    },
    "empty is undefined": function(reduce) {
      assert.strictEqual(reduce.empty, undefined);
    },
    "is not pyramidal": function(reduce) {
      assert.isTrue(!reduce.pyramidal);
    },
    "returns the median value": function(reduce) {
      assert.equal(reduce([1, 2, 3, 2, 1]), 2);
      assert.equal(reduce([1, 2, 4, 2, 1, 4, 4, 4]), 3);
    },
    "sorts input in-place": function(reduce) {
      var values = [1, 2, 3, 2, 1];
      reduce(values);
      assert.deepEqual(values, [1, 1, 2, 2, 3]);
    },
    "ignores undefined and NaN": function(reduce) {
      assert.equal(reduce([1, NaN, 3, undefined, 0]), 0);
    }
  },

  "min": {
    topic: function() {
      return reduces.min;
    },
    "empty is undefined": function(reduce) {
      assert.strictEqual(reduce.empty, undefined);
    },
    "is pyramidal": function(reduce) {
      assert.isTrue(reduce.pyramidal);
    },
    "returns the minimum value": function(reduce) {
      assert.equal(reduce([1, 2, 3, 2, 1]), 1);
    },
    "ignores undefined and NaN": function(reduce) {
      assert.equal(reduce([1, NaN, 3, undefined, 0]), 0);
    },
    "compares using natural order": function(reduce) {
      assert.equal(reduce([2, 10, 3]), 2);
      assert.equal(reduce(["2", "10", 3]), 3); // "2" > "10", 3 > "2"
      assert.equal(reduce([3, "2", "10"]), "10"); // 3 > "2", "2" > "10"
    },
    "returns the first of equal values": function(reduce) {
      assert.strictEqual(reduce([1, new Number(1)]), 1);
    }
  },

  "sum": {
    topic: function() {
      return reduces.sum;
    },
    "empty is zero": function(reduce) {
      assert.strictEqual(reduce.empty, 0);
    },
    "is pyramidal": function(reduce) {
      assert.isTrue(reduce.pyramidal);
    },
    "returns the sum of values": function(reduce) {
      assert.equal(reduce([1, 2, 3, 2, 1]), 9);
      assert.equal(reduce([1, 2, 4, 2, 1, 4, 4, 4]), 22);
    },
    "does not ignore undefined and NaN": function(reduce) {
      assert.isNaN(reduce([1, NaN, 3, undefined, 0]));
    }
  }
});

suite.export(module);

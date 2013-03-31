var vows = require("vows"),
    assert = require("assert"),
    test = require("./helpers"),
    types = require("../lib/cube/types"),
    mongodb = require("mongodb");

var suite = vows.describe("types");

suite.addBatch(test.batch({
  topic: function(test) {
    return types(test.db);
  },

  "types": {
    "returns collection cache for a given database": function(types) {
      assert.equal(typeof types, "function");
    },
    "each typed collection has events and metrics": function(types) {
      var collection = types("random"),
          keys = [];
      for (var key in collection) {
        keys.push(key);
      }
      keys.sort();
      assert.deepEqual(keys, ["events", "metrics"]);
      assert.isTrue(collection.events instanceof mongodb.Collection);
      assert.isTrue(collection.metrics instanceof mongodb.Collection);
      assert.equal(collection.events.collectionName, "random_events");
      assert.equal(collection.metrics.collectionName, "random_metrics");
    },
    "memoizes cached collections": function(types) {
      assert.strictEqual(types("random"), types("random"));
    }
  }
}));

suite.export(module);

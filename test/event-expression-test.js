var vows = require("vows"),
    assert = require("assert"),
    parser = require("../lib/cube/event-expression");

var suite = vows.describe("event-expression");

suite.addBatch({

  "a simple event expression, test": {
    topic: parser.parse("test"),
    "has the expected event type, test": function(e) {
      assert.equal(e.type, "test");
    },
    "does not load any event data fields": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {});
    },
    "does not apply any event data filters": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test");
    }
  },

  "an expression with a single field": {
    topic: parser.parse("test(i)"),
    "loads the specified event data field": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {"d.i": 1});
    },
    "ignores events that do not have the specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$exists: true}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test(i)");
    }
  },

  "an expression with multiple fields": {
    topic: parser.parse("test(i, j)"),
    "loads the specified event data fields": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {"d.i": 1, "d.j": 1});
    },
    "ignores events that do not have the specified fields": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$exists: true}, "d.j": {$exists: true}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test(i, j)");
    }
  },

  "an expression with a filter on the requested field": {
    topic: parser.parse("test(i).gt(i, 42)"),
    "loads the specified event data fields": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {"d.i": 1});
    },
    "only filters using the explicit filter; existence is implied": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test(i).gt(i, 42)");
    }
  },

  "an expression with filters on different fields": {
    topic: parser.parse("test.gt(i, 42).eq(j, \"foo\")"),
    "does not load fields that are only filtered": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {});
    },
    "has the expected filters on each specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42}, "d.j": "foo"});
    }
  },

  "an expression with multiple filters on the same field": {
    topic: parser.parse("test.gt(i, 42).le(i, 52)"),
    "combines multiple filters on the specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42, $lte: 52}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test.gt(i, 42).le(i, 52)");
    }
  },

  "an expression with range and exact filters on the same field": {
    topic: parser.parse("test.gt(i, 42).eq(i, 52)"),
    "ignores range filters, taking only the exact filter": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": 52});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test.gt(i, 42).eq(i, 52)");
    }
  },

  "an expression with exact and range filters on the same field": {
    topic: parser.parse("test.eq(i, 52).gt(i, 42)"),
    "ignores range filters, taking only the exact filter": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": 52});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test.eq(i, 52).gt(i, 42)");
    }
  },

  "an expression with an object data accessor": {
    topic: parser.parse("test(i.j)"),
    "loads the specified event data field": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {"d.i.j": 1});
    },
    "ignores events that do not have the specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i.j": {$exists: true}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test(i.j)");
    }
  },

  "an expression with an array data accessor": {
    topic: parser.parse("test(i[0])"),
    "loads the specified event data field": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {"d.i": 1});
    },
    "ignores events that do not have the specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$exists: true}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test(i[0])");
    }
  },

  "an expression with an elaborate data accessor": {
    topic: parser.parse("test(i.j[0].k)"),
    "loads the specified event data field": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {"d.i.j.k": 1});
    },
    "ignores events that do not have the specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i.j.k": {$exists: true}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test(i.j[0].k)");
    }
  },

  "an expression with an elaborate filter": {
    topic: parser.parse("test.gt(i.j[0].k, 42)"),
    "does not load fields that are only filtered": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {});
    },
    "has the expected filter": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i.j.0.k": {$gt: 42}});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "test.gt(i.j[0].k, 42)");
    }
  },

  "filters": {
    "the eq filter results in a simple query filter": function() {
      var filter = {};
      parser.parse("test.eq(i, 42)").filter(filter);
      assert.deepEqual(filter, {"d.i": 42});
    },
    "the gt filter results in a $gt query filter": function(e) {
      var filter = {};
      parser.parse("test.gt(i, 42)").filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42}});
    },
    "the ge filter results in a $gte query filter": function(e) {
      var filter = {};
      parser.parse("test.ge(i, 42)").filter(filter);
      assert.deepEqual(filter, {"d.i": {$gte: 42}});
    },
    "the lt filter results in an $lt query filter": function(e) {
      var filter = {};
      parser.parse("test.lt(i, 42)").filter(filter);
      assert.deepEqual(filter, {"d.i": {$lt: 42}});
    },
    "the le filter results in an $lte query filter": function(e) {
      var filter = {};
      parser.parse("test.le(i, 42)").filter(filter);
      assert.deepEqual(filter, {"d.i": {$lte: 42}});
    },
    "the ne filter results in an $ne query filter": function(e) {
      var filter = {};
      parser.parse("test.ne(i, 42)").filter(filter);
      assert.deepEqual(filter, {"d.i": {$ne: 42}});
    },
    "the re filter results in a $regex query filter": function(e) {
      var filter = {};
      parser.parse("test.re(i, \"foo\")").filter(filter);
      assert.deepEqual(filter, {"d.i": {$regex: "foo"}});
    },
    "the in filter results in a $in query filter": function(e) {
      var filter = {};
      parser.parse("test.in(i, [\"foo\", 42])").filter(filter);
      assert.deepEqual(filter, {"d.i": {$in: ["foo", 42]}});
    }
  }

});

suite.export(module);

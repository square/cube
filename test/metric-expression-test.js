var vows = require("vows"),
    assert = require("assert"),
    parser = require("../lib/cube/metric-expression");

var suite = vows.describe("metric-expression");

suite.addBatch({

  "a simple unary expression, sum(test)": {
    topic: parser.parse("sum(test)"),
    "is unary (has no associated binary operator)": function(e) {
      assert.isUndefined(e.op);
    },
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
    "has the expected reduce": function(e) {
      assert.equal(e.reduce, "sum");
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test)");
    },
    "returns the value one for any event": function(e) {
      assert.equal(e.value(), 1);
    }
  },

  "an expression with a simple data accessor": {
    topic: parser.parse("sum(test(i))"),
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
    "returns the specified field value": function(e) {
      assert.equal(e.value({d: {i: 42}}), 42);
      assert.equal(e.value({d: {i: -1}}), -1);
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test(i))");
    }
  },

  "an expression with a compound data accessor": {
    topic: parser.parse("sum(test(i + i * i - 2))"),
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
    "computes the specified value expression": function(e) {
      assert.equal(e.value({d: {i: 42}}), 1804);
      assert.equal(e.value({d: {i: -1}}), -2);
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test(i + i * i - 2))");
    }
  },

  "an expression with a data accessor and a filter on the same field": {
    topic: parser.parse("sum(test(i).gt(i, 42))"),
    "loads the specified event data field": function(e) {
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
      assert.equal(e.source, "sum(test(i).gt(i, 42))");
    }
  },

  "an expression with filters on different fields": {
    topic: parser.parse("sum(test.gt(i, 42).eq(j, \"foo\"))"),
    "does not load fields that are only filtered": function(e) {
      var fields = {};
      e.fields(fields);
      assert.deepEqual(fields, {});
    },
    "has the expected filters on each specified field": function(e) {
      var filter = {};
      e.filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42}, "d.j": "foo"});
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test.gt(i, 42).eq(j, \"foo\"))");
    }
  },

  "an expression with an object data accessor": {
    topic: parser.parse("sum(test(i.j))"),
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
    "computes the specified value expression": function(e) {
      assert.equal(e.value({d: {i: {j: 42}}}), 42);
      assert.equal(e.value({d: {i: {j: -1}}}), -1);
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test(i.j))");
    }
  },

  "an expression with an array data accessor": {
    topic: parser.parse("sum(test(i[0]))"),
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
    "computes the specified value expression": function(e) {
      assert.equal(e.value({d: {i: [42]}}), 42);
      assert.equal(e.value({d: {i: [-1]}}), -1);
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test(i[0]))");
    }
  },

  "an expression with an elaborate data accessor": {
    topic: parser.parse("sum(test(i.j[0].k))"),
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
    "computes the specified value expression": function(e) {
      assert.equal(e.value({d: {i: {j: [{k: 42}]}}}), 42);
      assert.equal(e.value({d: {i: {j: [{k: -1}]}}}), -1);
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "sum(test(i.j[0].k))");
    }
  },

  "a compound expression of two unary expressions": {
    topic: parser.parse("sum(foo(2)) + sum(bar(3))"),
    "is compound (has an associated binary operator)": function(e) {
      assert.equal(e.op.name, "add");
    },
    "has the expected left expression": function(e) {
      var filter = {}, fields = {};
      e.left.filter(filter);
      e.left.fields(fields);
      assert.deepEqual(filter, {});
      assert.deepEqual(fields, {});
      assert.equal(e.left.source, "sum(foo(2))");
      assert.equal(e.left.type, "foo");
      assert.equal(e.left.reduce, "sum");
      assert.equal(e.left.value(), 2);
    },
    "has the expected right expression": function(e) {
      var filter = {}, fields = {};
      e.right.filter(filter);
      e.right.fields(fields);
      assert.deepEqual(filter, {});
      assert.deepEqual(fields, {});
      assert.equal(e.right.source, "sum(bar(3))");
      assert.equal(e.right.type, "bar");
      assert.equal(e.right.reduce, "sum");
      assert.equal(e.right.value(), 3);
    },
    "does not have a source": function(e) {
      assert.isUndefined(e.source);
    },
    "computes the specified value expression": function(e) {
      assert.equal(e.op(2, 3), 5)
    }
  },

  "a compound expression of three unary expressions": {
    topic: parser.parse("sum(foo(2)) + median(bar(3)) + max(baz(qux))"),
    "has the expected subexpression sources": function(e) {
      assert.isUndefined(e.source);
      assert.equal(e.left.source, "sum(foo(2))");
      assert.isUndefined(e.right.source);
      assert.equal(e.right.left.source, "median(bar(3))");
      assert.equal(e.right.right.source, "max(baz(qux))");
    }
  },

  "a negated unary expression": {
    topic: parser.parse("-sum(foo)"),
    "negates the specified value expression": function(e) {
      assert.equal(e.value(), -1)
    },
    "has the expected source": function(e) {
      assert.equal(e.source, "-sum(foo)");
    }
  },

  "constant expressions": {
    topic: parser.parse("-4"),
    "has a constant value": function(e) {
      assert.equal(e.value(), -4)
    },
    "does not have a source": function(e) {
      assert.isUndefined(e.source);
    }
  },

  "filters": {
    "multiple filters on the same field are combined": function() {
      var filter = {};
      parser.parse("sum(test.gt(i, 42).le(i, 52))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42, $lte: 52}});
    },
    "given range and exact filters, range filters are ignored": function() {
      var filter = {};
      parser.parse("sum(test.gt(i, 42).eq(i, 52))").filter(filter);
      assert.deepEqual(filter, {"d.i": 52});
    },
    "given exact and range filters, range filters are ignored": function() {
      var filter = {};
      parser.parse("sum(test.eq(i, 52).gt(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": 52});
    },
    "the eq filter results in a simple query filter": function() {
      var filter = {};
      parser.parse("sum(test.eq(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": 42});
    },
    "the gt filter results in a $gt query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.gt(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$gt: 42}});
    },
    "the ge filter results in a $gte query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.ge(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$gte: 42}});
    },
    "the lt filter results in an $lt query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.lt(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$lt: 42}});
    },
    "the le filter results in an $lte query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.le(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$lte: 42}});
    },
    "the ne filter results in an $ne query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.ne(i, 42))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$ne: 42}});
    },
    "the re filter results in a $regex query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.re(i, \"foo\"))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$regex: "foo"}});
    },
    "the in filter results in a $in query filter": function(e) {
      var filter = {};
      parser.parse("sum(test.in(i, [\"foo\", 42]))").filter(filter);
      assert.deepEqual(filter, {"d.i": {$in: ["foo", 42]}});
    }
  }

});

suite.export(module);

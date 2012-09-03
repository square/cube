'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    cube        = require("../");

var suite = vows.describe("collector");

var server_options = { 'http-port': test_helper.get_port() };

suite.addBatch(
  test_helper.with_server(server_options, cube.collector.register, {

  "POST /event/put with invalid JSON": {
    topic: test_helper.request({method: "POST", path: "/1.0/event/put"}, "This ain't JSON.\n"),
    "responds with status 400": function(response) {
      assert.equal(response.statusCode, 400);
      assert.deepEqual(JSON.parse(response.body), {error: "SyntaxError: Unexpected token T"});
    }
  },
  "POST /event/put with a JSON object": {
    topic: test_helper.request({method: "POST", path: "/1.0/event/put"}, JSON.stringify({
      type: "test",
      time: new Date,
      data: {
        foo: "bar"
      }
    })),
    "responds with status 400": function(response) {
      assert.equal(response.statusCode, 400);
      assert.deepEqual(JSON.parse(response.body), {error: "TypeError: Object #<Object> has no method 'forEach'"});
    }
  },
  "POST /event/put with a JSON array": {
    topic: test_helper.request({method: "POST", path: "/1.0/event/put"}, JSON.stringify([{
      type: "test",
      time: new Date,
      data: {
        foo: "bar"
      }
    }])),
    "responds with status 200": function(response) {
      assert.equal(response.statusCode, 200);
      assert.deepEqual(JSON.parse(response.body), {});
    }
  },
  "POST /event/put with a JSON number": {
    topic: test_helper.request({method: "POST", path: "/1.0/event/put"}, JSON.stringify(42)),
    "responds with status 400": function(response) {
      assert.equal(response.statusCode, 400);
      assert.deepEqual(JSON.parse(response.body), {error: "TypeError: Object 42 has no method 'forEach'"});
    }
  },
  "POST /event/put without an associated time": {
    topic: test_helper.request({method: "POST", path: "/1.0/event/put"}, JSON.stringify([{
      type: "test",
      data: {
        foo: "bar"
      }
    }])),
    "responds with status 200": function(response) {
      assert.equal(response.statusCode, 200);
      assert.deepEqual(JSON.parse(response.body), {});
    }
  }
}));

suite.export(module);

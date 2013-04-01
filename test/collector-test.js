var vows = require("vows"),
    assert = require("assert"),
    cube = require("../"),
    test = require("./helpers");

var suite = vows.describe("collector");

var server = cube.server(test.config),
    port = test.config["http-port"];

console.log('collector port %s', port);

server.register = cube.collector.register;

server.start();

suite.addBatch(test.batch({
  "POST /event/put with invalid JSON": {
    topic: test.request({method: "POST", port: port, path: "/1.0/event/put"}, "This ain't JSON.\n"),
    "responds with status 400": function(response) {
      assert.equal(response.statusCode, 400);
      assert.deepEqual(JSON.parse(response.body), {error: "SyntaxError: Unexpected token T"});
    }
  }
}));

suite.addBatch(test.batch({
  "POST /event/put with a JSON object": {
    topic: test.request({method: "POST", port: port, path: "/1.0/event/put"}, JSON.stringify({
      type: "test",
      time: new Date(),
      data: {
        foo: "bar"
      }
    })),
    "responds with status 400": function(response) {
      assert.equal(response.statusCode, 400);
      assert.deepEqual(JSON.parse(response.body), {error: "TypeError: Object #<Object> has no method 'forEach'"});
    }
  }
}));

suite.addBatch(test.batch({
  "POST /event/put with a JSON array": {
    topic: test.request({method: "POST", port: port, path: "/1.0/event/put"}, JSON.stringify([{
      type: "test",
      time: new Date(),
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

suite.addBatch(test.batch({
  "POST /event/put with a JSON number": {
    topic: test.request({method: "POST", port: port, path: "/1.0/event/put"}, JSON.stringify(42)),
    "responds with status 400": function(response) {
      assert.equal(response.statusCode, 400);
      assert.deepEqual(JSON.parse(response.body), {error: "TypeError: Object 42 has no method 'forEach'"});
    }
  }
}));

suite.addBatch(test.batch({
  "POST /event/put without an associated time": {
    topic: test.request({method: "POST", port: port, path: "/1.0/event/put"}, JSON.stringify([{
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

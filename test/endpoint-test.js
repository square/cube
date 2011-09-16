var vows = require("vows"),
    assert = require("assert"),
    http = require("http"),
    test = require("./test"),
    endpoint = require("../lib/cube/server/endpoint");

var suite = vows.describe("endpoint");

var port = ++test.port,
    server = http.createServer(endpoint.file("../client/semicolon.js", "../client/semicolon.js"));

server.listen(port, "127.0.0.1");

suite.addBatch({
  "file": {
    "GET": {
      topic: test.request({method: "GET", port: port}),
      "the status should be 200": function(response) {
        assert.equal(response.statusCode, 200);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["content-type"], "text/javascript;charset=utf-8");
        assert.equal(response.headers["content-length"], 2);
        assert.ok(new Date(response.headers["date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["last-modified"]) > Date.UTC(2011, 0, 1));
      },
      "the expected content should be returned": function(response) {
        assert.equal(response.body, ";;");
      }
    },
    "GET If-Modified-Since": {
      topic: test.request({method: "GET", port: port, headers: {"if-modified-since": new Date(2101, 0, 1).toUTCString()}}),
      "the status should be 304": function(response) {
        assert.equal(response.statusCode, 304);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["content-type"], "text/javascript;charset=utf-8");
        assert.ok(!("Content-Length" in response.headers));
        assert.ok(new Date(response.headers["date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["last-modified"]) > Date.UTC(2011, 0, 1));
      },
      "no content should be returned": function(response) {
        assert.equal(response.body, "");
      }
    },
    "HEAD": {
      topic: test.request({method: "HEAD", port: port, headers: {"if-modified-since": new Date(2001, 0, 1).toUTCString()}}),
      "the status should be 200": function(response) {
        assert.equal(response.statusCode, 200);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["content-type"], "text/javascript;charset=utf-8");
        assert.ok(!("Content-Length" in response.headers));
        assert.ok(new Date(response.headers["date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["last-modified"]) > Date.UTC(2011, 0, 1));
      },
      "no content should be returned": function(response) {
        assert.equal(response.body, "");
      }
    }
  }
});

suite.export(module);

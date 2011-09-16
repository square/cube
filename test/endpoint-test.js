var vows = require("vows"),
    assert = require("assert"),
    endpoint = require("../lib/cube/server/endpoint");

var suite = vows.describe("endpoint");

suite.addBatch({
  "file": {
    "GET": {
      topic: testFile("GET", {}),
      "the status should be 200": function(response) {
        assert.equal(response.status, 200);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["Content-Type"], "text/javascript;charset=utf-8");
        assert.equal(response.headers["Content-Length"], 2);
        assert.ok(new Date(response.headers["Date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["Last-Modified"]) > Date.UTC(2011, 0, 1));
      },
      "the expected content should be returned": function(response) {
        assert.equal(response.body, ";;");
      }
    },
    "GET If-Modified-Since": {
      topic: testFile("GET", {"if-modified-since": new Date(2101, 0, 1).toUTCString()}),
      "the status should be 304": function(response) {
        assert.equal(response.status, 304);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["Content-Type"], "text/javascript;charset=utf-8");
        assert.ok(!("Content-Length" in response.headers));
        assert.ok(new Date(response.headers["Date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["Last-Modified"]) > Date.UTC(2011, 0, 1));
      },
      "no content should be returned": function(response) {
        assert.equal(response.body, "");
      }
    },
    "HEAD": {
      topic: testFile("HEAD", {"if-modified-since": new Date(2001, 0, 1).toUTCString()}),
      "the status should be 200": function(response) {
        assert.equal(response.status, 200);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["Content-Type"], "text/javascript;charset=utf-8");
        assert.ok(!("Content-Length" in response.headers));
        assert.ok(new Date(response.headers["Date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["Last-Modified"]) > Date.UTC(2011, 0, 1));
      },
      "no content should be returned": function(response) {
        assert.equal(response.body, "");
      }
    }
  }
});

function testFile(method, headers) {
  return function() {
    var file = endpoint.file("../client/semicolon.js", "../client/semicolon.js"),
        request = {headers: headers, method: method},
        response = {writeHead: head, write: write, end: end},
        status,
        body = "",
        cb = this.callback;

    function head(s, h) {
      status = s;
      headers = h;
    }

    function write(data) {
      body += data;
    }

    function end() {
      cb(null, {status: status, headers: headers, body: body});
    }

    file(request, response);
  };
}

suite.export(module);

var vows        = require("vows"),
    assert      = require("assert"),
    http        = require("http"),
    test_helper = require("./test_helper"),
    cube        = require("../");

var suite = vows.describe("server");

var server_options = { 'http-port': test_helper.get_port() }
function frontend_components(db, endpoints) {
};

suite.addBatch(
  test_helper.with_server(server_options, frontend_components, {
  "file": {
    "GET": {
      topic: test_helper.request({path: "/semicolon.js", method: "GET"}),
      "the status should be 200": function(response) {
        assert.equal(response.statusCode, 200);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["content-type"], "text/javascript");
        assert.equal(response.headers["content-length"], 1);
        assert.ok(new Date(response.headers["date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["last-modified"]) > Date.UTC(2011, 0, 1));
      },
      "the expected content should be returned": function(response) {
        assert.equal(response.body, ";");
      }
    },
    "GET If-Modified-Since": {
      topic: test_helper.request({path: "/semicolon.js", method: "GET", headers: {"if-modified-since": new Date(2101, 0, 1).toUTCString()}}),
      "the status should be 304": function(response) {
        assert.equal(response.statusCode, 304);
      },
      "the expected headers should be set": function(response) {
        assert.ok(!("Content-Length" in response.headers));
        assert.ok(new Date(response.headers["date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["last-modified"]) > Date.UTC(2011, 0, 1));
      },
      "no content should be returned": function(response) {
        assert.equal(response.body, "");
      }
    },
    "HEAD": {
      topic: test_helper.request({path: "/semicolon.js", method: "HEAD", headers: {"if-modified-since": new Date(2001, 0, 1).toUTCString()}}),
      "the status should be 200": function(response) {
        assert.equal(response.statusCode, 200);
      },
      "the expected headers should be set": function(response) {
        assert.equal(response.headers["content-type"], "text/javascript");
        assert.ok(!("Content-Length" in response.headers));
        assert.ok(new Date(response.headers["date"]) > Date.UTC(2011, 0, 1));
        assert.ok(new Date(response.headers["last-modified"]) > Date.UTC(2011, 0, 1));
      },
      "no content should be returned": function(response) {
        assert.equal(response.body, "");
      }
    }
  }
}));

suite.export(module);

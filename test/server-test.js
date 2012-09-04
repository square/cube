'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    cube        = require("../"),
    endpoint    = cube.endpoint,
    collector   = cube.collector,
    metalog     = cube.metalog;

var suite = vows.describe("server");

var bucket         = { udped: [], httped: [], websocked: [] };
var now_ish        = Date.now();
var example = {
  for_http: { type: "monotreme", time: now_ish, data: { echidnae:   4 } },
  for_udp:  { type: "monotreme", time: now_ish, data: { platypodes: 9 } } };

var server_options = {
  'http-port': test_helper.get_port(),
  'udp-port':  test_helper.get_port()
};
function dummy_server(db, endpoints){
  endpoints.udp = function(req, cb){
    // metalog.info('rcvd_udp', { req: req });
    bucket.udped.push(req);
  };
  endpoints.http.push(
    endpoint('POST', '/1.0/test', collector._post(function(req, cb){
      metalog.info('rcvd_http', { req: req });
      bucket.httped.push(req);
    })));
}

suite.addBatch(
  test_helper.with_server(server_options, dummy_server, {

  http: {
    topic: test_helper.request({path: "/1.0/test", method: "POST"}, JSON.stringify([example.for_http])),
    '': {
      topic: test_helper.delaying_topic,
      'sends data to registered putter': function(response){
        assert.deepEqual(bucket.httped.pop(), example.for_http);
        assert.isEmpty(bucket.httped);
      }
    }
  },

  udp: {
    topic: test_helper.udp_request(example.for_udp),
    '': function(){
      assert.deepEqual(bucket.udped.pop(), example.for_udp);
      assert.isEmpty(bucket.udped);
    }
  },

  file: {
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

suite['export'](module);

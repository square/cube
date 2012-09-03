'use strict';

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require("./test_helper"),
    cube        = require("../");

var suite = vows.describe("evaluator");

var server_options = { 'http-port': test_helper.get_port() }
function frontend_components() {
  cube.evaluator.register.apply(this, arguments);
  cube.visualizer.register.apply(this, arguments);
};

// suite.addBatch(
//   test_helper.with_server(server_options, frontend_components, {
//
//   "POST /event/put with invalid JSON": {
//     topic: test_helper.request({method: "POST", path: "/1.0/event/put"}, "This ain't JSON.\n"),
//     "responds with status 400": function(response) {
//       assert.equal(response.statusCode, 400);
//       assert.deepEqual(JSON.parse(response.body), {error: "SyntaxError: Unexpected token T"});
//     }
//   }
// }));

suite.export(module);

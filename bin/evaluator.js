'use strict';

var options = require("../config/cube").include('evaluator'),
    cube = require("../"),
    server = cube.server(options);

server.register = function(db, endpoints) {
  cube.evaluator.register(db, endpoints);
  cube.visualizer.register(db, endpoints);
};

server.start();

'use strict';

var cube = require("../"),
    server = cube.server('evaluator');

server
  .use(cube.evaluator.register)
  .use(cube.visualizer.register)
  .start();

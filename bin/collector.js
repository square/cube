'use strict';

var cube = require("../"),
    server = cube.server('collector');

server
  .use(cube.collector.register)
  .start();

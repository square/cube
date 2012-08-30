var options = require("../config/cube").include("collector"),
    cube = require("../"),
    server = cube.server(options);

server.register = function(db, endpoints) {
  cube.collector.register(db, endpoints);
};

server.start();

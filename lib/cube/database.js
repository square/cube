var mongodb = require("mongodb");

var database = module.exports = {};

function createServer(options) {
  if (options["mongo-hosts"]) {
    var mongoServers = [];
    for (var i = 0; i < options["mongo-hosts"].length; i++) {
      var server_opt = options["mongo-hosts"][i];
      mongoServers.push(new mongodb.Server(server_opt[0], parseInt(server_opt[1],10), server_opt[2]));
    }
    return new mongodb.ReplSetServers(mongoServers, { read_secondary: true });
  }
  var server_options = options["mongo-server-options"] || { auto_reconnect: true };
  return new mongodb.Server(options["mongo-host"], options["mongo-port"], server_options);
}

// Open MongoDB driver given Cube options:
// {
//   "mongo-host": "localhost",
//   "mongo-port": "27017",
//   "mongo-database": "cube",
//   "mongo-username": null,
//   "mongo-password": null,
// }
// (defaults are shown)
database.open = function(options, callback) {
  var mongo = createServer(options),
      db_options = options["mongo-database-options"] || { safe: false },
      db = new mongodb.Db(options["mongo-database"] || "cube", mongo, db_options);
  db.open(function(error) {
    if (error) {
      return callback(error);
    }
    if (options["mongo-username"]) {
      db.authenticate(options["mongo-username"], options["mongo-password"], function(error, success) {
        if (error) {
          return callback(error);
        } else if (!success) {
          return callback(new Error("authentication failed"));
        }
        return callback(null, db);
      });
    } else {
      return callback(null, db);
    }
  });
};
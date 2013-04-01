var mongodb = require("mongodb");

var database = module.exports = {};

// open MongoDB driver given Cube options:
// {
//   "mongo-host": "",
//   "mongo-port": "",
//   "mongo-database": "",
//   "mongo-username": "",
//   "mongo-password": "",
// }
database.open = function(options, callback) {
  var server_options = { auto_reconnect: true },
      mongo = new mongodb.Server(options["mongo-host"], options["mongo-port"], server_options),
      db_options = { safe: false },
      db = new mongodb.Db(options["mongo-database"], mongo, db_options);
  db.open(function(error) {
    if (error) {
      return callback(error);
    }
    if ("mongo-username" in options) {
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
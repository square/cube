var mongodb = require("mongodb");

var database = module.exports = {};

// Opens MongoDB driver given connection URL and optional options:
//
// {
//   "mongo-url": "",
//   "mongo-options": {
//     "db": { "safe": false },
//     "server": { "auto_reconnect": true },
//     "replSet": { "read_secondary": true }
//   }
// }
//
// See http://docs.mongodb.org/manual/reference/connection-string/ for details.
// You can also specify a Replica Set this way.
//
database.open = function(config, callback) {
  var url = config["mongo-url"] || database.config2url(config),
      options = config["mongo-options"] || database.config2options(config);
  return mongodb.Db.connect(url, options, callback);
};

//
// For backwards-compatibility you can specify a connection to a single Mongo(s) as follows:
//
// {
//   "mongo-host": "localhost",
//   "mongo-port": "27017",
//   "mongo-server-options": { "auto_reconnect": true },
//   "mongo-database": "cube",
//   "mongo-database-options": { "safe": false },
//   "mongo-username": null,
//   "mongo-password": null,
// }
// (defaults are shown)
//
database.config2url = function(config) {
  var user = config["mongo-username"],
      pass = config["mongo-password"],
      host = config["mongo-host"] || "localhost",
      port = config["mongo-port"] || 27017,
      name = config["mongo-database"] || "cube",
      auth = user ? user+":"+pass+"@" : "";
  return "mongodb://"+auth+host+":"+port+"/"+name;
};

database.config2options = function(config) {
  return {
    db: config["mongo-database-options"] || { safe: false },
    server: config["mongo-server-options"] || { auto_reconnect: true },
    replSet: { read_secondary: true }
  };
};
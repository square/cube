var endpoint = require("./endpoint");

exports.register = function(db, endpoints) {
  endpoints.ws.push(
    endpoint.exact("/1.0/event/get", require("./event").getter(db)),
    endpoint.exact("/1.0/metric/get", require("./metric").getter(db))
  );
};

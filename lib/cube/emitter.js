var url = require("url"),
    http = require("./emitter-http"),
    udp = require("./emitter-udp"),
    ws = require("./emitter-ws");

// returns an emmiter for the given URL; handles http://, udp:// or ws:// protocols
module.exports = function(u) {
  var emitter;
  u = url.parse(u);
  switch (u.protocol) {
    case "udp:": emitter = udp; break;
    case "ws:": case "wss:": emitter = ws; break;
    case "http:": emitter = http; break;
  }
  return emitter(u.protocol, u.hostname, u.port);
};

var util = require("util"),
    url = require("url"),
    udp = require("./emitter-udp"),
    ws = require("./emitter-ws");

module.exports = function(u) {
  var emitter;
  u = url.parse(u);
  switch (u.protocol) {
    case "udp:": emitter = udp; break;
    case "ws:": case "wss:": emitter = ws; break;
  }
  return emitter(u.protocol, u.hostname, u.port);
};

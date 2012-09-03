'use strict';

//
// emitter - writes events to the collector.
//
// URL's scheme determines UDP, websocket or HTTP emitter, as appropriate.
//

var util = require("util"),
    url = require("url"),
    http = require("./emitter-http"),
    udp = require("./emitter-udp"),
    ws = require("./emitter-ws");

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

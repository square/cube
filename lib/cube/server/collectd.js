exports.putter = function(putter) {
  var values = {},
      queue = [],
      flushInterval,
      flushDelay = 5000;

  function store(value, i, event, name) {
    var v1 = value.values[i];
    switch (value.dstypes[i]) {
      case "gauge": {
        event[name] = v1;
        break;
      }
      case "derive": {
        var k = value.host
            + "/" + value.plugin + "/" + value.plugin_instance
            + "/" + value.type + "/" + value.type_instance
            + "/" + name;
        event[name] = k in values
            ? -(values[k] - (values[k] = v1))
            : (values[k] = v1, 0);
        break;
      }
    }
  }

  flushInterval = setInterval(function() {
    var hosts = {},
        latest = Date.now() - 2 * flushDelay, // to coalesce
        retries = [];

    queue.forEach(function(value) {
      if (value.time > latest) {
        retries.push(value);
      } else {
        var host = hosts[value.host] || (hosts[value.host] = {}),
            event = host[value.time] || (host[value.time] = {});
        event = event[value.plugin] || (event[value.plugin] = {host: value.host});
        if (value.plugin_instance) event = event[value.plugin_instance] || (event[value.plugin_instance] = {});
        if (value.type != value.plugin) event = event[value.type] || (event[value.type] = {});
        if (value.values.length == 1) store(value, 0, event, value.type_instance);
        else value.values.forEach(function(d, i) { store(value, i, event, value.dsnames[i]); });
      }
    });

    queue = retries;

    for (var host in hosts) {
      for (var time in hosts[host]) {
        for (var type in hosts[host][time]) {
          putter({
            type: "collectd_" + type,
            time: new Date(+time),
            data: hosts[host][time][type]
          });
        }
      }
    }
  }, flushDelay);

  return function(request, response) {
    var content = "";
    request.on("data", function(chunk) {
      content += chunk;
    });
    request.on("end", function() {
      var future = Date.now() / 1e3 + 1e9;
      JSON.parse(content).forEach(function(value) {
        var time = value.time;
        if (time > future) time /= 1073741824;
        value.time = Math.round(time) * 1e3;
        queue.push(value);
      });
      response.writeHead(200);
      response.end();
    });
  };
};


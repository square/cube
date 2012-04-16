exports.putter = function(putter) {
  var valuesByKey = {};

  // Converts a collectd value list to a Cube event.
  function event(values) {
    var root = {host: values.host},
        data = root,
        parent,
        key;

    // The plugin and type are required. If the type is the same as the plugin,
    // then ignore the type (for example, memory/memory and load/load).
    parent = data, data = data[key = values.plugin] || (data[values.plugin] = {});
    if (values.type != values.plugin) parent = data, data = data[key = values.type] || (data[values.type] = {});

    // The plugin_instance and type_instance are optional.
    if (values.plugin_instance) root.plugin = values.plugin_instance;
    if (values.type_instance) root.type = values.type_instance;

    // If only a single value is specified, then don't store a map of named
    // values; just store the single value using the type_instance name (e.g.,
    // memory/memory-inactive, df-root/df_complex-used). Otherwise, iterate over
    // the received values and store them as a map.
    if (values.values.length == 1) parent[key] = value(0);
    else values.dsnames.forEach(function(d, i) { data[d] = value(i); });

    // For "derive" events, we must compute the delta since the last event.
    function value(i) {
      var d = values.values[i];
      switch (values.dstypes[i]) {
        case "derive": {
          var key =  values.host + "/" + values.plugin + "/" + values.plugin_instance + "/" + values.type + "/" + values.type_instance + "/" + values.dsnames[i],
              value = key in valuesByKey ? valuesByKey[key] : d;
          valuesByKey[key] = d;
          d -= value;
          break;
        }
      }
      return d;
    }

    return {
      type: "collectd",
      time: new Date(+values.time),
      data: root
    };
  }

  return function(request, response) {
    var content = "";
    request.on("data", function(chunk) {
      content += chunk;
    });
    request.on("end", function() {
      var future = Date.now() / 1e3 + 1e9;
      JSON.parse(content).forEach(function(values) {
        var time = values.time;
        if (time > future) time /= 1073741824;
        values.time = Math.round(time) * 1e3;
        putter(event(values));
      });
      response.writeHead(200);
      response.end();
    });
  };
};

cube.piece.type.area = function(board) {
  var timeout,
      data = [],
      dt0;

  var area = cube.piece(board)
      .on("size", resize)
      .on("serialize", serialize)
      .on("deserialize", deserialize);

  var div = d3.select(area.node())
      .classed("area", true);

  if (mode == "edit") {
    div.append("h3")
        .attr("class", "title")
        .text("Area Chart");

    var query = div.append("textarea")
        .attr("class", "query")
        .attr("placeholder", "query expression…")
        .on("keyup.area", querychange)
        .on("focus.area", area.focus)
        .on("blur.area", area.blur);

    var time = div.append("div")
        .attr("class", "time")
        .text("Time Range:")
      .append("select")
        .on("change.area", area.edit)
        .on("focus.area", area.focus)
        .on("blur.area", area.blur);

    time.selectAll("option")
        .data([
          {description: "1 Day / 5-Minute", value: 864e5 + "/" + 3e5},
          {description: "3 Days / 5-Minute", value: 3 * 864e5 + "/" + 3e5},
          {description: "7 Days / Hour", value: 7 * 864e5 + "/" + 36e5},
          {description: "14 Days / Hour", value: 14 * 864e5 + "/" + 36e5},
          {description: "30 Days / Hour", value: 30 * 864e5 + "/" + 36e5},
          {description: "30 Days / Day", value: 30 * 864e5 + "/" + 864e5}
        ])
      .enter().append("option")
        .property("selected", function(d, i) { return i == 1; })
        .attr("value", cube_piece_areaValue)
        .text(function(d) { return d.description; });
  } else {
    var m = [6, 40, 14, 10], // top, right, bottom, left margins
        socket;

    var svg = div.append("svg:svg");

    var x = d3.time.scale(),
        y = d3.scale.linear(),
        xAxis = d3.svg.axis().scale(x).orient("bottom").tickSubdivide(true),
        yAxis = d3.svg.axis().scale(y).orient("right");

    var a = d3.svg.area()
        .interpolate("step-after")
        .x(function(d) { return x(d.time); })
        .y0(function(d) { return y(0); })
        .y1(function(d) { return y(d.value); });

    var l = d3.svg.line()
        .interpolate("step-after")
        .x(function(d) { return x(d.time); })
        .y(function(d) { return y(d.value); });

    var g = svg.append("svg:g")
        .attr("transform", "translate(" + m[3] + "," + m[0] + ")");

    g.append("svg:g").attr("class", "y axis").call(yAxis);
    g.append("svg:path").attr("class", "area");
    g.append("svg:g").attr("class", "x axis").call(xAxis);
    g.append("svg:path").attr("class", "line");
  }

  function resize() {
    var transition = area.transition();

    if (mode == "edit") {
      var innerSize = area.innerSize();

      transition.select(".query")
          .style("width", innerSize[0] - 12 + "px")
          .style("height", innerSize[1] - 58 + "px");

      transition.select(".time select")
          .style("width", innerSize[0] - 100 + "px");

    } else {
      var z = board.squareSize(),
          w = area.size()[0] * z - m[1] - m[3],
          h = area.size()[1] * z - m[0] - m[2];

      x.range([0, w]);
      y.range([h, 0]);

      // Adjust the ticks based on the current chart dimensions.
      xAxis.ticks(w / 80).tickSize(-h, 0);
      yAxis.ticks(h / 25).tickSize(-w, 0);

      transition.select("svg")
          .attr("width", w + m[1] + m[3])
          .attr("height", h + m[0] + m[2]);

      transition.select(".area")
          .attr("d", a(data));

      transition.select(".x.axis")
          .attr("transform", "translate(0," + h + ")")
          .call(xAxis)
        .select("path")
          .attr("transform", "translate(0," + (y(0) - h) + ")");

      transition.select(".y.axis")
          .attr("transform", "translate(" + w + ",0)")
          .call(yAxis);

      transition.select(".line")
          .attr("d", l(data));
    }
  }

  function redraw() {
    if (data.length > 1) data[data.length - 1].value = data[data.length - 2].value;

    var z = board.squareSize(),
        h = area.size()[1] * z - m[0] - m[2],
        min = d3.min(data, cube_piece_areaValue),
        max = d3.max(data, cube_piece_areaValue);

    if ((min < 0) && (max < 0)) max = 0;
    else if ((min > 0) && (max > 0)) min = 0;
    y.domain([min, max]).nice();

    div.select(".area").attr("d", a(data));
    div.select(".y.axis").call(yAxis.tickFormat(cube_piece_format(y.domain())));
    div.select(".x.axis").call(xAxis).select("path").attr("transform", "translate(0," + (y(0) - h) + ")");
    div.select(".line").attr("d", l(data));
    return true;
  }

  function querychange() {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(area.edit, 750);
  }

  function serialize(json) {
    var t = time.property("value").split("/");
    json.type = "area";
    json.query = query.property("value");
    json.time = {range: +t[0], step: +t[1]};
  }

  function deserialize(json) {
    if (mode == "edit") {
      query.property("value", json.query);
      time.property("value", json.time.range + "/" + json.time.step);
    } else {
      var dt1 = json.time.step,
          t1 = new Date(Math.floor(Date.now() / dt1) * dt1),
          t0 = new Date(t1 - json.time.range),
          d0 = x.domain(),
          d1 = [t0, t1];

      if (dt0 != dt1) {
        data = [];
        dt0 = dt1;
      }

      if (d0 != d1 + "") {
        x.domain(d1);
        resize();
        var times = data.map(cube_piece_areaTime);
        data = data.slice(d3.bisectLeft(times, t0), d3.bisectLeft(times, t1));
        data.push({time: t1, value: 0});
      }

      if (timeout) timeout = clearTimeout(timeout);
      if (socket) socket.close();
      socket = new WebSocket("ws://" + location.host + "/1.0/metric/get");
      socket.onopen = load;
      socket.onmessage = store;

      function load() {
        timeout = setTimeout(function() {
          socket.send(JSON.stringify({
            expression: json.query,
            start: t0,
            stop: t1,
            step: dt1
          }));
          timeout = setTimeout(function() {
            deserialize(json);
          }, t1 - Date.now() + dt1 + 4500 + 1000 * Math.random());
        }, 500);
      }

      // TODO use a skip list to insert more efficiently
      // TODO compute contiguous segments on the fly
      function store(message) {
        var d = JSON.parse(message.data),
            i = d3.bisectLeft(data.map(cube_piece_areaTime), d.time = new Date(d.time));
        if (i < 0 || data[i].time - d.time) {
          if (d.value != null) {
            data.splice(i, 0, d);
          }
        } else if (d.value == null) {
          data.splice(i, 1);
        } else {
          data[i] = d;
        }
        d3.timer(redraw);
      }
    }
  }

  area.copy = function() {
    return board.add(cube.piece.type.area);
  };

  resize();

  return area;
};

function cube_piece_areaTime(d) {
  return d.time;
}

function cube_piece_areaValue(d) {
  return d.value;
}

var cube_piece_formatNumber = d3.format(".2r");

function cube_piece_format(domain) {
  var prefix = d3.formatPrefix(Math.max(-domain[0], domain[1]), 2);
  return function(value) {
    return cube_piece_formatNumber(value * prefix.scale) + prefix.symbol;
  };
}

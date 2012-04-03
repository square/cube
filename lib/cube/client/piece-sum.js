cube.piece.type.sum = function(board) {
  var timeout,
      socket,
      data = 0,
      format = d3.format(".2s");

  var sum = cube.piece(board)
      .on("size", resize)
      .on("serialize", serialize)
      .on("deserialize", deserialize);

  var div = d3.select(sum.node())
      .classed("sum", true);

  if (mode == "edit") {
    div.append("h3")
        .attr("class", "title")
        .text("Rolling Sum");

    var query = div.append("textarea")
        .attr("class", "query")
        .attr("placeholder", "query expression…")
        .on("keyup.sum", querychange)
        .on("focus.sum", sum.focus)
        .on("blur.sum", sum.blur);

    var time = div.append("div")
        .attr("class", "time")
        .text("Time Range:");

    time.append("input")
        .property("value", 1440);

    time.append("select").selectAll("option")
        .data([
          {description: "Seconds @ 10", value: 1e4},
          {description: "Minutes @ 5", value: 3e5},
          {description: "Hours", value: 36e5},
          {description: "Days", value: 864e5},
          {description: "Weeks", value: 6048e5},
          {description: "Months", value: 2592e6}
        ])
      .enter().append("option")
        .property("selected", function(d, i) { return i == 1; })
        .attr("value", cube_piece_areaValue)
        .text(function(d) { return d.description; });

    time.selectAll("input,select")
        .on("change.sum", sum.edit)
        .on("focus.sum", sum.focus)
        .on("blur.sum", sum.blur)
  }

  function resize() {
    var innerSize = sum.innerSize(),
        transition = sum.transition();

    if (mode == "edit") {
      transition.select(".query")
          .style("width", innerSize[0] - 12 + "px")
          .style("height", innerSize[1] - 58 + "px");

      transition.select(".time select")
          .style("width", innerSize[0] - 174 + "px");
    } else {
      transition
          .style("font-size", innerSize[0] / 5 + "px")
          .style("line-height", innerSize[1] + "px")
          .text(format(data));
    }
  }

  function redraw() {
    div.text(format(data));
    return true;
  }

  function querychange() {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(sum.edit, 750);
  }

  function serialize(json) {
    var step = +time.select("select").property("value"),
        range = time.select("input").property("value") * cube_piece_areaMultipler(step);
    json.type = "sum";
    json.query = query.property("value");
    json.time = {range: range, step: step};
  }

  function deserialize(json) {
    if (!json.time.range) json.time = {range: json.time, step: 3e5};
    if (mode == "edit") {
      query.property("value", json.query);
      time.select("input").property("value", json.time.range / cube_piece_areaMultipler(json.time.step));
      time.select("select").property("value", json.time.step);
    } else {
      var dt = json.time.step,
          t1 = new Date(Math.floor(Date.now() / dt) * dt),
          t0 = new Date(t1 - json.time.range);

      data = 0;

      if (timeout) timeout = clearTimeout(timeout);
      if (socket) socket.close();
      socket = new WebSocket("ws://" + location.host + "/1.0/metric/get");
      socket.onopen = load;
      socket.onmessage = store;

      function load() {
        socket.send(JSON.stringify({
          expression: json.query,
          start: cube_time(t0),
          stop: cube_time(t1),
          step: dt
        }));
        timeout = setTimeout(function() {
          deserialize(json);
        }, t1 - Date.now() + dt + 4500 + 1000 * Math.random());
      }

      function store(message) {
        data += JSON.parse(message.data).value;
        d3.timer(redraw);
      }
    }
  }

  sum.copy = function() {
    return board.add(cube.piece.type.sum);
  };

  resize();

  return sum;
};

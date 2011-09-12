cube.piece.type.sum = function(board) {
  var timeout,
      socket,
      data = 0,
      format = d3.format(",.0f");

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
        .style("margin-left", "4px")
        .style("height", "20px")
        .style("border-radius", "4px")
        .style("border", "solid 1px #ccc")
        .style("resize", "none")
        .attr("placeholder", "query expression…")
        .on("keyup.sum", querychange)
        .on("focus.sum", sum.focus)
        .on("blur.sum", sum.blur);

    var time = div.append("div")
        .attr("class", "time")
        .style("margin-top", "1px")
        .style("margin-left", "4px")
        .style("line-height", "20px")
        .text("Time Range:")
      .append("select")
        .style("margin-right", "4px")
        .style("float", "right")
        .style("height", "20px")
        .style("border-radius", "4px")
        .style("border", "solid 1px #ccc")
        .on("change.sum", sum.edit)
        .on("focus.sum", sum.focus)
        .on("blur.sum", sum.blur);

    time.selectAll("option")
        .data([
          {description: "5 Minutes / 5-Minute", value: 3e5 + "/" + 3e5},
          {description: "1 Hour / 5-Minute", value: 36e5 + "/" + 3e5},
          {description: "1 Hour / Hour", value: 36e5 + "/" + 36e5},
          {description: "1 Day / 5-Minute", value: 864e5 + "/" + 3e5},
          {description: "1 Day / Hour", value: 864e5 + "/" + 36e5},
          {description: "1 Day / Day", value: 864e5 + "/" + 864e5}
        ])
      .enter().append("option")
        .property("selected", function(d, i) { return i == 1; })
        .attr("value", cube_piece_areaValue)
        .text(function(d) { return d.description; });
  } else {
    div
        .style("text-align", "right");
  }

  function resize() {
    var innerSize = sum.innerSize(),
        transition = sum.transition();

    if (mode == "edit") {
      transition.select(".query")
          .style("width", innerSize[0] - 12 + "px")
          .style("height", innerSize[1] - 58 + "px");

      transition.select(".time select")
          .style("width", innerSize[0] - 100 + "px");
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
    var t = time.property("value").split("/");
    json.type = "sum";
    json.query = query.property("value");
    json.time = {range: +t[0], step: +t[1]};
  }

  function deserialize(json) {
    if (!json.time.range) json.time = {range: json.time, step: 3e5};
    if (mode == "edit") {
      query.property("value", json.query);
      time.property("value", json.time.range + "/" + json.time.step);
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
          start: t0,
          stop: t1,
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

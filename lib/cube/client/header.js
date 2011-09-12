cube.header = function(board) {
  var header = {};

  var div = document.createElement("div");

  var selection = d3.select(div)
      .attr("class", "header");

  var left = selection.append("div")
      .attr("class", "left");

  left.append("a")
      .attr("href", "/" + board.id)
    .append("button")
      .text("View");

  left.append("a")
      .attr("href", "/" + board.id + "/edit")
    .append("button")
      .text("Edit");

  var viewers = selection.append("div")
      .attr("class", "right");

  board.on("view", function(e) {
    viewers.text(e.count > 1 ? e.count - 1 + " other" + (e.count > 2 ? "s" : "") + " viewing" : null);
  });

  header.node = function() {
    return div;
  };

  if (mode == "view") {
    var shown = false;

    d3.select(window)
        .on("mouseout", mouseout)
        .on("mousemove", mousemove);

    function show(show) {
      if (show != shown) {
        d3.select(div.parentNode).transition()
            .style("top", ((shown = show) ? 0 : -60) + "px");
      }
    }

    function mouseout() {
      if (d3.event.relatedTarget == null) show(false);
    }

    function mousemove() {
      if (d3.event.pageY > 120) show(false);
      else if (d3.event.pageY < 60) show(true);
    }
  }

  return header;
};

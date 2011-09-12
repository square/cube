cube.palette = function(board) {
  var palette = {};

  var g = document.createElementNS(d3.ns.prefix.svg, "g");

  var type = d3.select(g)
      .attr("class", "palette")
    .selectAll(".piece-type")
      .data(d3.entries(cube.piece.type))
    .enter().append("svg:g")
      .attr("class", "piece-type")
      .on("mousedown", mousedown);

  type.append("svg:rect");

  type.append("svg:text")
      .attr("dy", ".35em")
      .attr("text-anchor", "middle")
      .text(function(d) { return d.key; });

  board
      .on("squareSize", resize)
      .on("squareRadius", resize);

  function resize() {
    var size = board.squareSize(),
        radius = board.squareRadius();

    type
        .attr("transform", function(d, i) { return "translate(" + (i * size + size / 2) + "," + (size / 2) + ")"; })
      .select("rect")
        .attr("x", -size / 2)
        .attr("y", -size / 2)
        .attr("width", size)
        .attr("height", size)
        .attr("rx", radius)
        .attr("ry", radius);
  }

  function mousedown(d) {
    var piece = board.add(d.value),
        pieceSize = piece.size(),
        squareSize = board.squareSize(),
        mouse = d3.svg.mouse(g);

    piece.position([
      mouse[0] / squareSize - pieceSize[0] / 2,
      mouse[1] / squareSize - pieceSize[1] / 2 - 1.5
    ]);

    // Simulate mousedown on the piece to start dragging.
    var div = d3.select(piece.node());
    div.each(div.on("mousedown.piece"));
  }

  palette.node = function() {
    return g;
  };

  resize();

  return palette;
};

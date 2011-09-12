cube.squares = function(board) {
  var squares = {};

  var g = document.createElementNS(d3.ns.prefix.svg, "g");

  d3.select(g)
      .attr("class", "squares");

  board
      .on("size", resize)
      .on("squareSize", resize)
      .on("squareRadius", resize);

  function resize() {
    var boardSize = board.size(),
        squareSize = board.squareSize(),
        squareRadius = board.squareRadius();

    var square = d3.select(g).selectAll(".square")
        .data(d3.range(boardSize[0] * boardSize[1])
        .map(function(d) {return { x: d % boardSize[0], y: d / boardSize[0] | 0}; }));

    square.enter().append("svg:rect")
        .attr("class", "square");

    square
        .attr("rx", squareRadius)
        .attr("ry", squareRadius)
        .attr("class", function(d, i) { return (i - d.y & 1 ? "black" : "white") + " square"; })
        .attr("x", function(d) { return d.x * squareSize; })
        .attr("y", function(d) { return d.y * squareSize; })
        .attr("width", squareSize)
        .attr("height", squareSize);

    square.exit().remove();
  }

  squares.node = function() {
    return g;
  };

  resize();

  return squares;
};

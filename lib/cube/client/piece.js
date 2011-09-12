cube.piece = function(board) {
  var piece = {},
      size = [8, 3],
      position = [0, 0],
      padding = 4;

  var event = d3.dispatch(
    "position",
    "size",
    "move",
    "edit",
    "serialize",
    "deserialize"
  );

  var div = document.createElement("div"),
      selection = d3.select(div),
      transition = selection;

  var selection = d3.select(div)
      .attr("class", "piece");

  if (mode == "edit") {
    selection
        .attr("tabindex", 1)
        .on("keydown.piece", keydown)
        .on("mousedown.piece", mousedrag)
      .selectAll(".resize")
        .data(["n", "e", "s", "w", "nw", "ne", "se", "sw"])
      .enter().append("div")
        .attr("class", function(d) { return "resize " + d; })
        .on("mousedown.piece", mouseresize);

    d3.select(window)
        .on("keydown.piece", cube_piece_keydown)
        .on("mousemove.piece", cube_piece_mousemove)
        .on("mouseup.piece", cube_piece_mouseup);
  }

  board
      .on("padding", resize)
      .on("squareSize", resize);

  event.position.add(resize);
  event.size.add(resize);
  event.deserialize.add(deserialize);

  function resize() {
    var squareSize = board.squareSize(),
        boardPadding = board.padding() | 0;

    piece.transition()
        .style("left", (padding + boardPadding + position[0] * squareSize) + "px")
        .style("top", (padding + boardPadding + (1.5 + position[1]) * squareSize) + "px")
        .style("width", size[0] * squareSize - 2 * padding + 1 + "px")
        .style("height", size[1] * squareSize - 2 * padding + 1 + "px");
  }

  function deserialize(x) {
    piece
        .size(x.size)
        .position(x.position);
  }

  function keydown() {
    if (d3.event.target !== this) return d3.event.stopPropagation();
    if (cube_piece_dragPiece) return;
    if (d3.event.keyCode === 8) {
      board.remove(piece);
      d3.event.preventDefault();
    }
  }

  piece.node = function() {
    return div;
  };

  piece.on = function(type, listener) {
    event[type].add(listener);
    return piece;
  };

  piece.off = function(type, listener) {
    event[type].remove(listener);
    return piece;
  };

  piece.size = function(x) {
    if (!arguments.length) return size;
    event.size.dispatch.call(piece, size = x);
    return piece;
  };

  piece.innerSize = function() {
    var squareSize = board.squareSize();
    return [
      size[0] * squareSize - 2 * padding,
      size[1] * squareSize - 2 * padding
    ];
  };

  piece.position = function(x) {
    if (!arguments.length) return position;
    event.position.dispatch.call(piece, position = x);
    return piece;
  };

  piece.toJSON = function() {
    var x = {id: piece.id, size: size, position: position};
    event.serialize.dispatch.call(piece, x);
    return x;
  };

  piece.fromJSON = function(x) {
    event.deserialize.dispatch.call(piece, x);
    return piece;
  };

  piece.edit = function() {
    event.edit.dispatch.call(piece);
    return piece;
  };

  function mousedrag() {
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(d3.event.target.tagName)) return;
    if (d3.event.target === this && d3.event.altKey) {
      cube_piece_dragPiece = piece.copy().fromJSON(piece.toJSON());
      cube_piece_dragPiece.node().focus();
    } else {
      d3.select(this).transition(); // cancel transition, if any
      this.parentNode.appendChild(this).focus();
      cube_piece_dragPiece = piece;
    }
    cube_piece_dragOrigin = [d3.event.pageX, d3.event.pageY];
    cube_piece_dragPosition = position.slice();
    cube_piece_dragSize = size.slice();
    cube_piece_dragBoard = board;
    cube_piece_mousemove();
  }

  function mouseresize(d) {
    cube_piece_dragResize = d;
  }

  piece.transition = function(x) {
    if (!arguments.length) return transition;
    if (x == null) {
      transition = selection;
    } else {
      transition = x.select(function() { return div; });
      d3.timer(function() {
        event.move.dispatch.call(piece);
        return transition = selection;
      });
    }
    return piece;
  };

  piece.focus = function() {
    selection.classed("active", true);
    return piece;
  };

  piece.blur = function() {
    selection.classed("active", false);
    return piece;
  };

  resize();

  return piece;
};

cube.piece.type = {};

var cube_piece_dragPiece,
    cube_piece_dragBoard,
    cube_piece_dragOrigin,
    cube_piece_dragPosition,
    cube_piece_dragSize,
    cube_piece_dragResize;

function cube_piece_mousePosition() {
  var squareSize = cube_piece_dragBoard.squareSize();
  return cube_piece_dragResize ? [
    cube_piece_dragPosition[0] + /w$/.test(cube_piece_dragResize) * Math.min(cube_piece_dragSize[0] - 5, (d3.event.pageX - cube_piece_dragOrigin[0]) / squareSize),
    cube_piece_dragPosition[1] + /^n/.test(cube_piece_dragResize) * Math.min(cube_piece_dragSize[1] - 3, (d3.event.pageY - cube_piece_dragOrigin[1]) / squareSize)
  ] : [
    cube_piece_dragPosition[0] + (d3.event.pageX - cube_piece_dragOrigin[0]) / squareSize,
    cube_piece_dragPosition[1] + (d3.event.pageY - cube_piece_dragOrigin[1]) / squareSize
  ];
}

function cube_piece_mouseSize() {
  var squareSize = cube_piece_dragBoard.squareSize();
  return cube_piece_dragResize ? [
    Math.max(5, cube_piece_dragSize[0] + (/e$/.test(cube_piece_dragResize) ? 1 : /w$/.test(cube_piece_dragResize) ? -1 : 0) * (d3.event.pageX - cube_piece_dragOrigin[0]) / squareSize),
    Math.max(3, cube_piece_dragSize[1] + (/^s/.test(cube_piece_dragResize) ? 1 : /^n/.test(cube_piece_dragResize) ? -1 : 0) * (d3.event.pageY - cube_piece_dragOrigin[1]) / squareSize)
  ] : cube_piece_dragSize;
}

function cube_piece_clamp(position, size) {
  var boardSize = cube_piece_dragBoard.size();
  if (cube_piece_dragResize) {
    if (/e$/.test(cube_piece_dragResize)) {
      size[0] = Math.max(0, Math.min(boardSize[0] - position[0], Math.round(size[0])));
    } else if (/w$/.test(cube_piece_dragResize)) {
      size[0] = Math.round(size[0] + position[0] - (position[0] = Math.max(0, Math.min(boardSize[0], Math.round(position[0])))));
    }
    if (/^s/.test(cube_piece_dragResize)) {
      size[1] = Math.max(0, Math.min(boardSize[1] - position[1], Math.round(size[1])));
    } else if (/^n/.test(cube_piece_dragResize)) {
      size[1] = Math.round(size[1] + position[1] - (position[1] = Math.max(0, Math.min(boardSize[1], Math.round(position[1])))));
    }
  } else {
    position[0] = Math.max(0, Math.min(boardSize[0] - size[0], Math.round(position[0])));
    position[1] = Math.max(0, Math.min(boardSize[1] - size[1], Math.round(position[1])));
  }
}

function cube_piece_mousemove() {
  if (cube_piece_dragPiece) {
    var position = cube_piece_mousePosition(),
        size = cube_piece_mouseSize();

    cube_piece_dragPiece
        .position(position.slice())
        .size(size.slice());

    cube_piece_clamp(position, size);

    var x0 = position[0],
        y0 = position[1],
        x1 = x0 + size[0],
        y1 = y0 + size[1];

    d3.select(cube_piece_dragBoard.node()).selectAll(".squares rect")
        .classed("shadow", function(d, i) { return d.x >= x0 && d.x < x1 && d.y >= y0 && d.y < y1; });

    d3.event.preventDefault();
  }
}

function cube_piece_mouseup() {
  if (cube_piece_dragPiece) {
    var position = cube_piece_mousePosition(),
        size = cube_piece_mouseSize();

    cube_piece_clamp(position, size);

    d3.select(cube_piece_dragBoard.node()).selectAll(".squares rect")
        .classed("shadow", false);

    cube_piece_dragPiece
        .transition(d3.transition().ease("elastic").duration(500))
        .position(position)
        .size(size);

    cube_piece_dragPiece =
    cube_piece_dragBoard =
    cube_piece_dragEvent =
    cube_piece_dragOrigin =
    cube_piece_dragPosition =
    cube_piece_dragSize =
    cube_piece_dragResize = null;
    d3.event.preventDefault();
  }
}

// Disable delete as the back key, since we use it to delete pieces.
function cube_piece_keydown() {
  if (d3.event.keyCode == 8) {
    d3.event.preventDefault();
  }
}

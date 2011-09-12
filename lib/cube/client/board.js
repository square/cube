cube.board = function(url, id) {
  var board = {id: cube_board_formatId(id)},
      socket,
      interval,
      pieceId = 0,
      palette,
      squares,
      pieces = [],
      size = [32, 18], // in number of squares
      squareSize = 40, // in pixels
      squareRadius = 4, // in pixels
      padding = 9.5; // half-pixel for crisp strokes

  var event = d3.dispatch(
    "size",
    "squareSize",
    "squareRadius",
    "padding",
    "view"
  );

  var svg = document.createElementNS(d3.ns.prefix.svg, "svg");

  d3.select(svg)
      .attr("class", "board");

  event.size.add(resize);
  event.squareSize.add(resize);
  event.squareRadius.add(resize);
  event.padding.add(resize);

  function message(message) {
    var e = JSON.parse(message.data);
    switch (e.type) {
      case "view": {
        event.view.dispatch.call(board, e);
        break;
      }
      case "add": {
        var piece = board.add(cube.piece.type[e.piece.type])
            .fromJSON(e.piece)
            .off("move", add)
            .on("move", move);

        d3.select(piece.node())
            .style("opacity", 1e-6)
          .transition()
            .duration(500)
            .style("opacity", 1);

        pieceId = Math.max(pieceId, piece.id = e.piece.id);
        break;
      }
      case "edit": {
        pieces.some(function(piece) {
          if (piece.id == e.piece.id) {
            piece
                .off("move", move)
                .transition(d3.transition().duration(500))
                .fromJSON(e.piece);

            // Renable events after transition starts.
            d3.timer(function() { piece.on("move", move); }, 250);
            return true;
          }
        });
        break;
      }
      case "move": {
        pieces.some(function(piece) {
          if (piece.id == e.piece.id) {
            piece
                .off("move", move)
                .transition(d3.transition().duration(500))
                .size(e.piece.size)
                .position(e.piece.position);

            // Bring to front.
            svg.parentNode.appendChild(piece.node());

            // Renable events after transition starts.
            d3.timer(function() { piece.on("move", move); }, 250);
            return true;
          }
        });
        break;
      }
      case "remove": {
        pieces.some(function(piece) {
          if (piece.id == e.piece.id) {
            board.remove(piece, true);
            return true;
          }
        });
        break;
      }
    }
  }

  function reopen() {
    if (socket) {
      pieces.slice().forEach(function(piece) { board.remove(piece, true); });
      socket.close();
    }
    socket = new WebSocket(url);
    socket.onopen = load;
    socket.onmessage = message;
    if (!interval) interval = setInterval(ping, 5000);
  }

  function load() {
    if (id && socket && socket.readyState == 1) {
      socket.send(JSON.stringify({type: "load", id: id}));
    }
  }

  function ping() {
    if (socket.readyState == 1) {
      socket.send(JSON.stringify({type: "ping", id: id}));
    } else if (socket.readyState > 1) {
      reopen();
    }
  }

  // A one-time listener to send an add event on mouseup.
  function add() {
    socket.send(JSON.stringify({type: "add", id: id, piece: this}));
    this.off("move", add).on("move", move);
  }

  function move() {
    socket.send(JSON.stringify({type: "move", id: id, piece: this}));
  }

  function edit() {
    socket.send(JSON.stringify({type: "edit", id: id, piece: this}));
  }

  function resize() {
    d3.select(svg)
        .attr("width", size[0] * squareSize + 2 * padding)
        .attr("height", (size[1] + 2) * squareSize + 2 * padding);

    d3.select(palette.node())
        .attr("transform", "translate(" + padding + "," + padding + ")");

    d3.select(squares.node())
        .attr("transform", "translate(" + padding + "," + (1.5 * squareSize + padding) + ")");
  }

  board.node = function() {
    return svg;
  };

  board.on = function(type, listener) {
    event[type].add(listener);
    return board;
  };

  board.off = function(type, listener) {
    event[type].remove(listener);
    return board;
  };

  board.size = function(x) {
    if (!arguments.length) return size;
    event.size.dispatch.call(board, size = x);
    return board;
  };

  board.squareSize = function(x) {
    if (!arguments.length) return squareSize;
    event.squareSize.dispatch.call(board, squareSize = x);
    return board;
  };

  board.squareRadius = function(x) {
    if (!arguments.length) return squareRadius;
    event.squareRadius.dispatch.call(board, squareRadius = x);
    return board;
  };

  board.padding = function(x) {
    if (!arguments.length) return padding;
    event.padding.dispatch.call(board, padding = x);
    return board;
  };

  board.add = function(type) {
    var piece = type(board);
    piece.id = ++pieceId;
    piece.on("move", add).on("edit", edit);
    svg.parentNode.appendChild(piece.node());
    pieces.push(piece);
    return piece;
  };

  board.remove = function(piece, silent) {
    piece.off("move", move).off("edit", edit);
    if (silent) {
      d3.select(piece.node())
          .style("opacity", 1)
        .transition()
          .duration(500)
          .style("opacity", 1e-6)
          .remove();
    } else {
      socket.send(JSON.stringify({type: "remove", id: id, piece: {id: piece.id}}));
      svg.parentNode.removeChild(piece.node());
    }
    pieces.splice(pieces.indexOf(piece), 1);
    return piece;
  };

  board.toJSON = function() {
    return {id: id, size: size, pieces: pieces};
  };

  svg.appendChild((palette = cube.palette(board)).node());
  svg.appendChild((squares = cube.squares(board)).node());
  resize();
  reopen();

  return board;
};

function cube_board_formatId(id) {
  id = id.toString(36);
  if (id.length < 6) id = new Array(7 - id.length).join("0") + id;
  return id;
}

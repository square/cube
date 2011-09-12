var url = require("url"),
    endpoint = require("./endpoint");

exports.register = function(db, endpoints) {
  endpoints.ws.push(
    endpoint.exact("/board", viewBoard(db))
  );
  endpoints.http.push(
    endpoint.exact("/", createBoard(db)),
    endpoint.re(/^\/[0-9][0-9a-z]{5}(\/edit)?$/, loadBoard(db)),
    endpoint.exact("/cube.js", endpoint.file(
      "lib/cube/client/start.js",
      "lib/cube/client/cube.js",
      "lib/cube/client/piece.js",
      "lib/cube/client/piece-area.js",
      "lib/cube/client/piece-sum.js",
      "lib/cube/client/piece-text.js",
      "lib/cube/client/palette.js",
      "lib/cube/client/squares.js",
      "lib/cube/client/board.js",
      "lib/cube/client/header.js",
      "lib/cube/client/end.js"
    )),
    endpoint.exact("/cube.css", endpoint.file(
      "lib/cube/client/body.css",
      "lib/cube/client/palette.css",
      "lib/cube/client/board.css",
      "lib/cube/client/piece.css"
    )),
    endpoint.exact("/d3/d3.js", endpoint.file(
      "node_modules/d3/d3.min.js",
      "lib/cube/client/semicolon.js",
      "node_modules/d3/d3.time.min.js"
    ))
  );
};

function createBoard(db) {
  var boards, max = parseInt("9zzzzy", 36);

  db.collection("boards", function(error, collection) {
    boards = collection;
  });

  return function random(request, response) {
    var id = (Math.random() * max | 0) + 1;
    boards.insert({_id: id}, {safe: true}, function(error) {
      if (error) {
        if (/^E11000/.test(error.message)) return random(request, response); // duplicate
        response.writeHead(500, {"Content-Type": "text/plain"});
        response.end("500 Server Error");
      } else {
        id = id.toString(36);
        if (id.length < 6) id = new Array(7 - id.length).join("0") + id;
        response.writeHead(302, {"Location": "http://" + request.headers["host"] + "/" + id + "/edit"});
        response.end();
      }
    });
  };
}

function loadBoard(db) {
  var boards,
      file = endpoint.file("lib/cube/client/visualizer.html");

  db.collection("boards", function(error, collection) {
    boards = collection;
  });

  return function random(request, response) {
    var id = parseInt(url.parse(request.url).pathname.substring(1), "36");
    boards.findOne({_id: id}, function(error, object) {
      if (object == null) {
        response.writeHead(404, {"Content-Type": "text/plain"});
        response.end("404 Not Found");
      } else {
        file(request, response);
      }
    });
  };
}

function viewBoard(db) {
  var boards,
      boardsByCallback = {},
      callbacksByBoard = {};

  db.collection("boards", function(error, collection) {
    boards = collection;
  });

  function dispatch(request, callback) {
    switch (request.type) {
      case "load": load(request, callback); break;
      case "add": add(request, callback); break;
      case "edit": case "move": move(request, callback); break;
      case "remove": remove(request, callback); break;
      default: callback({type: "error", status: 400}); break;
    }
  }

  function add(request, callback) {
    var boardId = request.id,
        callbacks = callbacksByBoard[boardId].filter(function(c) { return c.id != callback.id; });
    boards.update({_id: boardId}, {$push: {pieces: request.piece}});
    if (callbacks.length) emit(callbacks, {type: "add", piece: request.piece});
  }

  function move(request, callback) {
    var boardId = request.id,
        callbacks = callbacksByBoard[boardId].filter(function(c) { return c.id != callback.id; });
    boards.update({_id: boardId, "pieces.id": request.piece.id}, {$set: {"pieces.$": request.piece}});
    if (callbacks.length) emit(callbacks, {type: request.type, piece: request.piece});
  }

  function remove(request, callback) {
    var boardId = request.id,
        callbacks = callbacksByBoard[boardId].filter(function(c) { return c.id != callback.id; });
    boards.update({_id: boardId}, {$pull: {pieces: {id: request.piece.id}}});
    if (callbacks.length) emit(callbacks, {type: "remove", piece: {id: request.piece.id}});
  }

  function load(request, callback) {
    var boardId = boardsByCallback[callback.id],
        callbacks;

    // If callback was previously viewing to a different board, remove it.
    if (boardId) {
      callbacks = callbacksByBoard[boardId];
      callbacks.splice(callbacks.indexOf(callback), 1);
      if (callbacks.length) emit(callbacks, {type: "view", count: callbacks.length});
      else delete callbacksByBoard[boardId];
    }

    // Register that we are now viewing the new board.
    boardsByCallback[callback.id] = boardId = request.id;

    // If this board has other viewers, notify them.
    if (boardId in callbacksByBoard) {
      callbacks = callbacksByBoard[boardId];
      callbacks.push(callback);
      emit(callbacks, {type: "view", count: callbacks.length});
    } else {
      callbacks = callbacksByBoard[boardId] = [callback];
    }

    // Asynchronously load the requested board.
    boards.findOne({_id: boardId}, function(error, board) {
      if (board != null) {
        if (board.pieces) board.pieces.forEach(function(piece) {
          callback({type: "add", piece: piece});
        });
      } else {
        callback({type: "error", status: 404});
      }
    });
  }

  dispatch.close = function(callback) {
    var boardId = boardsByCallback[callback.id],
        callbacks;

    // If callback was viewing, remove it.
    if (boardId) {
      callbacks = callbacksByBoard[boardId];
      callbacks.splice(callbacks.indexOf(callback), 1);
      if (callbacks.length) emit(callbacks, {type: "view", count: callbacks.length});
      else delete callbacksByBoard[boardId];
      delete boardsByCallback[callback.id];
    }
  };

  return dispatch;
}

function emit(callbacks, event) {
  callbacks.forEach(function(callback) {
    callback(event);
  });
}

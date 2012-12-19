'use strict';

var url      = require("url"),
    path     = require("path"),
    endpoint = require("./endpoint"),
    metalog  = require('./metalog'),
    parser  = require("./metric-expression");

exports.register = function(db, endpoints) {
  endpoints.ws.push(
    endpoint(/^\/boards\/[a-z0-9\-_]+(\/edit)?$/i, viewBoard(db))
  );
};

function viewBoard(db) {
  var boards,
      boardsByCallback = {},
      callbacksByBoard = {};

  function dispatch(request, callback) {
    if (request.type != 'ping') metalog.info("viz_req", { is: request.type, bd: request.id, pc: callback.id });
    request.id = require('mongodb').ObjectID(request.id);

    db.collection("boards", function(error, collection) {
      boards = collection;
      switch (request.type) {
        case "load": load(request, callback); break;
        case "add":  add(request, callback); break;
        case "edit": case "move": move(request, callback); break;
        case "remove": remove(request, callback); break;
        default: callback({type: "error", status: 400}); break;
      }
    });
  }

  function check_authorization(request, action){
    if (request.authorized.admin) return true;
    metalog.info('viz_auth_no', { action: action, u: request.authorized });
    return false;
  }

  function add(request, callback) {
    if (! check_authorization(request, 'add')) return;

    var boardId = request.id,
        callbacks = callbacksByBoard[boardId].filter(function(c) { return c.id != callback.id; });
    boards.update({_id: boardId}, {$push: {pieces: request.piece}});
    if (callbacks.length) emit(callbacks, {type: "add", piece: request.piece});
  }

  function move(request, callback) {
    if (! check_authorization(request, 'move')) return;

    var boardId = request.id,
        callbacks = callbacksByBoard[boardId].filter(function(c) { return c.id != callback.id; }),
        response  = callbacksByBoard[boardId].filter(function(c) { return c.id == callback.id; }),
        error;
    boards.update({_id: boardId, "pieces.id": request.piece.id}, {$set: {"pieces.$": request.piece}});
    if (request.piece.query) {
      try { parser.parse(request.piece.query); }
      catch (e) { error = e.message + ' Line: ' + e.line + ', Column: ' + e.column; }
    }
    if (error) emit(response, { type: 'error', piece: request.piece, error: error });
    if (callbacks.length && !error) emit(callbacks, {type: request.type, piece: request.piece});
  }

  function remove(request, callback) {
    if (! check_authorization(request, 'remove')) return;

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
      if (board !== null) {
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

function resolve(file) {
  return path.join(__dirname, "../client", file);
}

function emit(callbacks, event) {
  callbacks.forEach(function(callback) {
    callback(event);
  });
}

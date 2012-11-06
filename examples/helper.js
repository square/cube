'use strict';
process.env.TZ = 'UTC';

var cube       = require("../"),
    metalog    = cube.metalog,
    cromulator = require("./random-emitter/cromulator"),
    un         = cromulator.un,
    event_mod  = require("../lib/cube/event"),
    models     = require("../lib/cube/models"), Event = models.Event;

var options = require("../config/cube").include('evaluator'),
    mongodb = require("mongodb"),
    mongo   = new mongodb.Server(options["mongo-host"], options["mongo-port"], options["mongo-server_options"]),
    db      = new mongodb.Db(options["mongo-database"], mongo, { native_parser: true }),
    putter, getter;

var type = 'doh';

db.open(function(error) {
  putter = event_mod.putter(db);
  getter = event_mod.getter(db);

  var emitter = cube.emitter("ws://127.0.0.1:1080");

  var helper = {
    emitter: emitter
  };

  // helper.invalidate = function(){
  //   helper.emitter.invalidate_range(type, Date.now - (20 * un.min), Date.now - (12 * un.min));
  // }

  metalog.info('emitter', {is: 'starting'});

  var thenish = 1346629570000;

  var ev = new Event(type, thenish, {value: 3});
  // emitter.send(ev.to_request());

  for (var ii = 0; ii <= 3; ii++){
    ev = new Event(type, thenish - ii * ii * 40000, {value: ii});
    putter(ev.to_request());
  }
  setTimeout(function(){ metalog.inspectify(event_mod.invalidator().tsets()); }, 200);
  setTimeout(function(){ event_mod.stop(); }, 1200);

  metalog.info('emitter', {is: 'stopping'});
  helper.emitter.close();
});

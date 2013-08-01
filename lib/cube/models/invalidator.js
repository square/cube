'use strict';

var _ = require("underscore"),
    metalog    = require('../metalog'),
    tiers      = require("../tiers"),
    Model      = require("../core_ext/model");


// How frequently to invalidate metrics after receiving events.
var invalidateInterval = 5000;

// Schedule deferred invalidation of metrics by type and tier.
function Invalidator(){
  this.type_tsets          = {},
  this.invalidate          = { $set: {i: true} },
  this.invalidationOptions = { multi: true, w: 0 };
}

Model.modelize(Invalidator);

function add(type, ev){
  var tt = this.type_tset(type);
  for (var tier in tiers){ tt[tier][tier*Math.floor(ev.time/tier)] = true; }
};

function flush(db, callback){
  var _this = this;
  _.each(_this.type_tsets, function(type_tset, type){
    db.metrics(type, function(error, collection){
      callback(error);

      _.each(type_tset, function(tset, tier){
        var times = dateify(tset);
        metalog.info("event_flush", { type: type, tier: tier, times: times });
        collection.update({ i: false, "_id.l": +tier, "_id.t": {$in: times}}, _this.invalidate, _this.invalidationOptions);
      });
    });
  });
};

function tsets(){ return _.mapHash(this.type_tsets, function(tt, type){ return _.mapHash(tt, dateify); }); };

Invalidator.setProperties({
  add:       { value: add       },
  flush:     { value: flush     },
  tsets:     { get: tsets       },
  type_tset: { value: type_tset }
});

function type_tset(type){
  if (! (type in this.type_tsets)) this.type_tsets[type] = _.mapHash(tiers, function(){ return {}; });;
  return this.type_tsets[type];
};
function dateify(tset){
  return _.map(_.keys(tset), function(time){
      return new Date(+time);
    }).sort(function(aa,bb){return aa-bb;});
}

Invalidator.flushers = {};
Invalidator.start_flusher = function(id, cb){ Invalidator.flushers[id] = setInterval(cb, invalidateInterval); };
Invalidator.stop_flusher  = function(id, on_stop){
  clearInterval(Invalidator.flushers[id]);
  delete Invalidator.flushers[id];
  if (on_stop) on_stop();
};

module.exports = Invalidator;

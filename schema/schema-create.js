db.createCollection("boards");

["random", "collectd_df", "collectd_load", "collectd_interface", "collectd_memory"].forEach(function(type) {
  var event = type + "_events", metric = type + "_metrics";
  db.createCollection(event);
  db[event].ensureIndex({t: 1});
  db.createCollection(metric, {capped: true, size: 1e7, autoIndexId: true});
  db[metric].ensureIndex({"i": 1, "_id.e": 1, "_id.l": 1, "_id.t": 1});
  db[metric].ensureIndex({"i": 1, "_id.l": 1, "_id.t": 1});
});

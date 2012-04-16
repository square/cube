// Much like db.collection, but caches the result for both events and metrics.
// Also, this is synchronous, since we are opening a collection unsafely.
module.exports = function(db) {
  var collections = {};
  return function(type) {
    var collection = collections[type];
    if (!collection) {
      collection = collections[type] = {};
      db.collection(type + "_events", function(error, events) {
        collection.events = events;
      });
      db.collection(type + "_metrics", function(error, metrics) {
        collection.metrics = metrics;
      });
    }
    return collection;
  };
};

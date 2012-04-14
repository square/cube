var info = exports.info = {};

exports.stat = function(name) {
  var i = 0,
      n = arguments.length,
      o = info[name],
      p;
  if (!o) o = info[name] = new stat();
  while (++i < n) {
    p = o;
    o = info[name += "." + arguments[i]];
    if (!o) o = info[name] = new stat(p);
  }
  return o;
};

function stat(parent) {
  this.value = 0;
  this.parent = parent;
}

stat.prototype.toJSON = function() {
  return this.value;
};

stat.prototype.add = function(value) {
  this.value = this.value + value >>> 0;
  if (this.parent) this.parent.add(value);
};

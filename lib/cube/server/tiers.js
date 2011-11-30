var tiers = module.exports = {};

var second = 1000,
    second20 = 20 * second,
    minute = 60 * second,
    minute5 = 5 * minute,
    hour = 60 * minute,
    day = 24 * hour,
    week = 7 * day,
    month = 30 * day,
    year = 365 * day;

tiers[second20] = {
  key: second20,
  floor: function(d) { return new Date(Math.floor(d / second20) * second20); },
  ceil: tier_ceil,
  step: function(d) { return new Date(+d + second20); }
};

tiers[minute5] = {
  key: minute5,
  floor: function(d) { return new Date(Math.floor(d / minute5) * minute5); },
  ceil: tier_ceil,
  step: function(d) { return new Date(+d + minute5); },
  next: tiers[second20],
  size: function() { return 15; }
};

tiers[hour] = {
  key: hour,
  floor: function(d) { return new Date(Math.floor(d / hour) * hour); },
  ceil: tier_ceil,
  step: function(d) { return new Date(+d + hour); },
  next: tiers[minute5],
  size: function() { return 12; }
};

tiers[day] = {
  key: day,
  floor: function(d) { return new Date(Math.floor(d / day) * day); },
  ceil: tier_ceil,
  step: function(d) { return new Date(+d + day); },
  next: tiers[hour],
  size: function() { return 24; }
};

tiers[week] = {
  key: week,
  floor: function(d) { (d = new Date(Math.floor(d / day) * day)).setUTCDate(d.getUTCDate() - d.getUTCDay()); return d; },
  ceil: tier_ceil,
  step: function(d) { return new Date(+d + week); },
  next: tiers[day],
  size: function() { return 7; }
};

tiers[month] = {
  key: month,
  floor: function(d) { return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)); },
  ceil: tier_ceil,
  step: function(d) { (d = new Date(d)).setUTCMonth(d.getUTCMonth() + 1); return d; },
  next: tiers[day],
  size: function(d) { return 32 - new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 32)).getUTCDate(); }
};

function tier_ceil(date) {
  return this.step(this.floor(new Date(date - 1)));
}

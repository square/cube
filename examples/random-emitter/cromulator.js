var metalog    = require("../../lib/cube/metalog");

var un = {}; un.sec = 1000; un.min = 60 * un.sec; un.hr = 60 * un.min;

var cromulator = {
  count:      0,
  step:       un.sec * 1,
  start:      Date.now(),
  jitter:     2 * un.sec,  // spread in event times
  visit_rate: 10.0,
  un:         un
};

var ev = {
  ramp:        0.0,
  walk:        0.0,
  characters: {
    homer: 324, bart: 263, lisa: 203, marge: 142, scratchy: 79, itchy: 79, maggie: 51, mr_burns: 49,
    ned_flanders: 39, milhouse: 38, skinner: 37, sideshow_mel: 31, willie: 30, quimby: 25, moe: 25,
    krusty: 24, nelson: 23, wiggum: 22, grampa: 22, frink: 19, apu: 16, sideshow_bob: 14,
    selma: 14, patty: 14, barney: 13, mrs_krabappel: 12, comic_book_guy: 12, martin: 10,
    dr_hibbert: 10, smithers: 9, ralph: 9, rev_lovejoy: 8, lionel_hutz: 8, fat_tony: 8, chalmers: 8,
    snake: 7, otto: 6, dr_nick: 6, cletus: 5, troy_mcclure: 4, todd: 3, rodd: 3, kent_brockman: 3 },
  characters_pool: []
};
for (var ch in ev.characters) { for (i=0; i < ev.characters[ch]; i++){ ev.characters_pool.push(ch) } };

// random element from a list
function rand_element(list){ return list[Math.floor(Math.random() * list.length)]; }
// Fuzzy sine wave
function sine(since, period, fuzz){
  return (Math.sin(Math.PI * since / period) + normal(0, fuzz));
}
// Normally-distributed variate with given average and standard deviation --  http://en.wikipedia.org/wiki/Box%E2%80%93Muller_transform
function normal(avg, stdev){ return avg + (stdev * Math.sqrt(-2 * Math.log(Math.random())) * Math.cos(2 * Math.PI * Math.random())); }
// Exponential variate; 0 < lambda, the rate of events -- http://en.wikipedia.org/wiki/Exponential_distribution#Generating_exponential_variates
function exprand(lambda){  return (- Math.log(Math.random()) / lambda); }
// Geometric variate; 0 < p < 1, the probability an event will happen
function geomrand(p){ return Math.floor(exprand( -Math.log(1-p) )); }

cromulator.report = function(stage, time){
  var stop = cromulator.stop || Date.now();
  return { is:      stage,
           start:   new Date(cromulator.start),
           stop:    new Date(stop),
           per_sec: (1000.0 / cromulator.step),
           qty:     (stop - cromulator.start) / cromulator.step,
           secs:    (stop - cromulator.start) / 1000,
           count:   cromulator.count,
           ago:     (Date.now() - time)/1000 };
};

cromulator.data_at = function(time){
  var stop = cromulator.stop || Date.now();
  var data = {
    sine_45m:  sine(time - cromulator.start, 45 * un.min, 0.2),
    sine_5m:   sine(time - cromulator.start,  5 * un.min, 0.1),
    walk:      ev.walk += (Math.random() - 0.5),
    ramp:      ev.ramp += (Math.random() * 20 * cromulator.step / (stop - cromulator.start)),
    visits:    exprand( un.sec * cromulator.visit_rate / cromulator.step ),
    who:       rand_element(ev.characters_pool),
    people:    [rand_element(ev.characters_pool), rand_element(ev.characters_pool), rand_element(ev.characters_pool)],
    _meta:     { time: time, stop: stop, start: cromulator.start, step: cromulator.step }
  };
  ++cromulator.count;
  return data;
};

cromulator.spread_time = function(time){ return new Date(time + (cromulator.jitter * (Math.random()-0.5)) ) };

module.exports = cromulator;

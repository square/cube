'use strict';

//
// endpoint -- router for requests.
//
// specify
// * optional HTTP method ("GET", "POST", etc)
// * path, a String or RegExp
// * dispatch, a callback to invoke
//

module.exports = function(method, path, dispatch) {
  var match;
  if (method instanceof RegExp) {
    dispatch = path, path = method;
    match    = function(p, m) { return path.test(p); };
  } else if (arguments.length < 3) {
    dispatch = path, path = method;
    match    = function(p)    { return p == path; };
  } else { // path is a string
    match    = function(p, m) { return m == method && p == path; };
  };
  return { match: match, dispatch: dispatch };
}

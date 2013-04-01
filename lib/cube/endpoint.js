// creates an endpoint with given HTTP method, URL path and dispatch (function)
// (method argument is optional)
// endpoints are evaluated in server.js and
// dispatch(request, response) is called if path/method matches
module.exports = function(method, path, dispatch) {
  return {
    match: arguments.length < 3
        ? (dispatch = path, path = method, function(p) { return p == path; })
        : function(p, m) { return m == method && p == path; },
    dispatch: dispatch
  };
}

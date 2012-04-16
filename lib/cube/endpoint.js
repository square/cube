module.exports = function(method, path, dispatch) {
  return {
    match: arguments.length < 3
        ? (dispatch = path, path = method, function(p) { return p == path; })
        : function(p, m) { return m == method && p == path; },
    dispatch: dispatch
  };
}

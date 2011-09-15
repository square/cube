var fs = require("fs"),
    url = require("url"),
    path = require("path"),
    cube = require("../");

exports.re = re;
exports.exact = exact;
exports.file = file;

var types = {
  html: "text/html",
  css: "text/css",
  js: "text/javascript"
};

function exact(method, path, dispatch) {
  return {
    match: arguments.length < 3
        ? (dispatch = path, path = method, function(p) { return p == path; })
        : function(p, m) { return m == method && p == path; },
    dispatch: dispatch
  };
}

function re(re, dispatch) {
  return {
    match: function(p) { return re.test(p); },
    dispatch: dispatch
  };
}

function file() {
  var files = Array.prototype.map.call(arguments, resolve),
      type = types[files[0].substring(files[0].lastIndexOf(".") + 1)];
  return function(request, response) {
    var modified = -Infinity,
        size = 0,
        n = files.length;

    files.forEach(function(file) {
      fs.stat(file, function(error, stats) {
        if (error) return fiveohoh(request, response);
        size += stats.size;
        var time = new Date(stats.mtime);
        if (time > modified) modified = time;
        if (!--n) respond();
      });
    });

    function respond() {
      var status = modified <= new Date(request.headers["if-modified-since"]) ? 304 : 200;

      response.writeHead(status, {
        "Content-Type": type + ";charset=utf-8",
        "Content-Length": size,
        "Last-Modified": modified.toUTCString()
      });

      if ((status === 200) && (request.method !== "HEAD")) {
        return read(0);
      }

      return response.end();
    }

    function read(i) {
      fs.readFile(files[i], "UTF-8", function(error, data) {
        if (error) return fiveohoh(request, response);
        response.write(data);
        if (i < files.length - 1) read(i + 1);
        else response.end();
      });
    }
  };
};

function resolve(name) {
  return path.join(__dirname, name);
}

function fiveohoh(request, response) {
  response.writeHead(500, {"Content-Type": "text/plain"});
  response.end("500 Server Error");
}

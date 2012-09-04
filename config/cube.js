var configs = {},
    metalog = require('../lib/cube/metalog');

metalog.send_events = false;

//
//Shared configuration
//
configs.common = {
  "mongo-host": "127.0.0.1",
  "mongo-port": 27017,
  "mongo-database": "dashpot_development",
  "mongo-username": null,
  "mongo-password": null,
  "mongo-server_options": {auto_reconnect: true, poolSize: 8, socketOptions: { noDelay: true }},
  
  "mongo-metrics":  {autoIndexId: true, capped: false            },
  "mongo-events":   {autoIndexId: true, capped: true,  size: 1e9 },
  
  // "horizons": {
  //   "calculation":          1000 * 60 * 60 * 2, // 2 hours
  //   "invalidation":         1000 * 60 * 60 * 1, // 1 hour
  // }
};


//
// Collector configuration
//
configs.collector = {
  "http-port": 1080,
  "udp-port": 1180,
  "authenticator": "allow_all"
}


//
// Evaluator configuration
//
configs.evaluator = {
  "http-port": 1081,
  // "authenticator":  "mongo_cookie"
  "authenticator": "allow_all"
}


//
// Warmer configuration
//
configs.warmer = {
  "warmer-interval": 1000 * 30,
  "warmer-tier":     1000 * 10
}

var options = {};
Object.defineProperty(options, "include", {
    enumerable: false,
    value: function() {
      for(var config_name in arguments){
        var config = configs[arguments[config_name]];
        for(var prop in config)
          if(config.hasOwnProperty(prop))
            options[prop] = config[prop];
      };
      return options;
    }
});

module.exports = options.include('common');

'use strict';

var cfg = module.exports = require('cfg').createConfig(),
  metalog = require('../lib/cube/metalog');

metalog.send_events = false;

//
// Common configuration
//

cfg.set('mongodb', {
  'mongo-host': '127.0.0.1',
  'mongo-port': 27017,
  'mongo-database': 'cube',
  'mongo-username': null,
  'mongo-password': null,
  'mongo-server-options': {
    auto_reconnect: true,
    poolSize: 8,
    socketOptions: {
      noDelay: true
    }
  },

  'mongo-metrics': {
    autoIndexId: true,
    capped: false,
    safe: false
  },

  'mongo-events': {
    autoIndexId: true,
    capped: true,
    size: 1e9,
    safe: false
  },

  'separate-events-database': true,

  'authentication-collection': 'users'
});

cfg.set('horizons', {
  'calculation': 1000 * 60 * 60 * 2, // 2 hours
  'invalidation': 1000 * 60 * 60 * 1, // 1 hour
  'forced_metric_expiration': 1000 * 60 * 60 * 24 * 7, // 7 days
});

cfg.set('collectd-mappings', {
  'snmp': {
    'if_octets': 'interface',
    'disk_octets': 'disk',
    'swap_io': 'swap',
    'swap': 'swap'
  }
});

cfg.set('collector', {
  'http-port': 1080,
  'udp-port': 1180,
  'authenticator': 'allow_all'
});

cfg.set('evaluator', {
  'http-port': 1081,
  'authenticator': 'allow_all'
});

cfg.set('warmer', {
  'warmer-interval': 1000 * 30,
  'warmer-tier': 1000 * 10
});


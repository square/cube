var vows = require("vows"),
    assert = require("assert"),
    tiers = require("../lib/cube/tiers");

var suite = vows.describe("tiers");

suite.addBatch({

  "tiers": {
    "contains exactly the expected tiers": function() {
      var keys = [];
      for (var key in tiers) {
        keys.push(+key);
      }
      keys.sort(function(a, b) { return a - b; });
      assert.deepEqual(keys, [1e4, 6e4, 3e5, 36e5, 864e5]);
    }
  },

  "second10": {
    topic: tiers[1e4],
    "has the key 1e4": function(tier) {
      assert.strictEqual(tier.key, 1e4);
    },
    "next is undefined": function(tier) {
      assert.isUndefined(tier.next);
    },
    "size is undefined": function(tier) {
      assert.isUndefined(tier.size);
    },

    "floor": {
      "rounds down to 10-seconds": function(tier) {
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 20)), utc(2011, 08, 02, 12, 00, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 21)), utc(2011, 08, 02, 12, 00, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 23)), utc(2011, 08, 02, 12, 00, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 39)), utc(2011, 08, 02, 12, 00, 30));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 40)), utc(2011, 08, 02, 12, 00, 40));
      },
      "does not modify the passed-in date": function(tier) {
        var date = utc(2011, 08, 02, 12, 00, 21);
        assert.deepEqual(tier.floor(date), utc(2011, 08, 02, 12, 00, 20));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 00, 21));
      }
    },

    "ceil": {
      "rounds up to 10-seconds": function(tier) {
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 20)), utc(2011, 08, 02, 12, 00, 20));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 21)), utc(2011, 08, 02, 12, 00, 30));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 23)), utc(2011, 08, 02, 12, 00, 30));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 39)), utc(2011, 08, 02, 12, 00, 40));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 40)), utc(2011, 08, 02, 12, 00, 40));
      },
      "does not modified the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 00, 21);
        assert.deepEqual(tier.ceil(date), utc(2011, 08, 02, 12, 00, 30));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 00, 21));
      }
    },

    "step": {
      "increments time by ten seconds": function(tier) {
        var date = utc(2011, 08, 02, 23, 59, 20);
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 59, 30));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 59, 40));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 59, 50));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 00, 10));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 00, 20));
      },
      "does not round the specified date": function(tier) {
        assert.deepEqual(tier.step(utc(2011, 08, 02, 12, 21, 23)), utc(2011, 08, 02, 12, 21, 33));
      },
      "does not modify the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 20, 00);
        assert.deepEqual(tier.step(date), utc(2011, 08, 02, 12, 20, 10));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 20, 00));
      }
    }
  },

  "minute": {
    topic: tiers[6e4],
    "has the key 6e4": function(tier) {
      assert.strictEqual(tier.key, 6e4);
    },
    "next is undefined": function(tier) {
      assert.isUndefined(tier.next);
    },
    "size is undefined": function(tier) {
      assert.isUndefined(tier.size);
    },

    "floor": {
      "rounds down to minutes": function(tier) {
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 20, 00)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 20, 01)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 12, 21));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 23, 00)), utc(2011, 08, 02, 12, 23));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 24, 59)), utc(2011, 08, 02, 12, 24));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 25, 00)), utc(2011, 08, 02, 12, 25));
      },
      "does not modify the passed-in date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21, 20);
        assert.deepEqual(tier.floor(date), utc(2011, 08, 02, 12, 21));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21, 20));
      }
    },

    "ceil": {
      "rounds up to minutes": function(tier) {
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 20, 00)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 20, 01)), utc(2011, 08, 02, 12, 21));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 12, 21));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 23, 00)), utc(2011, 08, 02, 12, 23));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 24, 59)), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 25, 00)), utc(2011, 08, 02, 12, 25));
      },
      "does not modified the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21, 20);
        assert.deepEqual(tier.ceil(date), utc(2011, 08, 02, 12, 22));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21, 20));
      }
    },

    "step": {
      "increments time by one minute": function(tier) {
        var date = utc(2011, 08, 02, 23, 45, 00);
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 46));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 47));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 48));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 49));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 50));
      },
      "does not round the specified date": function(tier) {
        assert.deepEqual(tier.step(utc(2011, 08, 02, 12, 21, 23)), utc(2011, 08, 02, 12, 22, 23));
      },
      "does not modify the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 20);
        assert.deepEqual(tier.step(date), utc(2011, 08, 02, 12, 21));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 20));
      }
    }
  },

  "minute5": {
    topic: tiers[3e5],
    "has the key 3e5": function(tier) {
      assert.strictEqual(tier.key, 3e5);
    },
    "next is undefined": function(tier) {
      assert.isUndefined(tier.next);
    },
    "size is undefined": function(tier) {
      assert.isUndefined(tier.size);
    },

    "floor": {
      "rounds down to 5-minutes": function(tier) {
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 20, 00)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 20, 01)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 23, 00)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 24, 59)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 25, 00)), utc(2011, 08, 02, 12, 25));
      },
      "does not modify the passed-in date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21);
        assert.deepEqual(tier.floor(date), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21));
      }
    },

    "ceil": {
      "rounds up to 5-minutes": function(tier) {
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 20, 00)), utc(2011, 08, 02, 12, 20));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 20, 01)), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 23, 00)), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 24, 59)), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 25, 00)), utc(2011, 08, 02, 12, 25));
      },
      "does not modified the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21, 00);
        assert.deepEqual(tier.ceil(date), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21));
      }
    },

    "step": {
      "increments time by five minutes": function(tier) {
        var date = utc(2011, 08, 02, 23, 45, 00);
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 50));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 55));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 05));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 10));
      },
      "does not round the specified date": function(tier) {
        assert.deepEqual(tier.step(utc(2011, 08, 02, 12, 21, 23)), utc(2011, 08, 02, 12, 26, 23));
      },
      "does not modify the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 20, 00);
        assert.deepEqual(tier.step(date), utc(2011, 08, 02, 12, 25));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 20));
      }
    }
  },

  "hour": {
    topic: tiers[36e5],
    "has the key 36e5": function(tier) {
      assert.strictEqual(tier.key, 36e5);
    },
    "next is the 5-minute tier": function(tier) {
      assert.equal(tier.next, tiers[3e5]);
    },
    "size is 12": function(tier) {
      assert.strictEqual(tier.size(), 12);
    },

    "floor": {
      "rounds down to hours": function(tier) {
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 00)), utc(2011, 08, 02, 12, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 00, 01)), utc(2011, 08, 02, 12, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 12, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 59, 59)), utc(2011, 08, 02, 12, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 13, 00, 00)), utc(2011, 08, 02, 13, 00));
      },
      "does not modify the passed-in date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21);
        assert.deepEqual(tier.floor(date), utc(2011, 08, 02, 12, 00));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21));
      }
    },

    "ceil": {
      "rounds up to hours": function(tier) {
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 00)), utc(2011, 08, 02, 12, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 00, 01)), utc(2011, 08, 02, 13, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 13, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 59, 59)), utc(2011, 08, 02, 13, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 13, 00, 00)), utc(2011, 08, 02, 13, 00));
      },
      "does not modified the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21, 00);
        assert.deepEqual(tier.ceil(date), utc(2011, 08, 02, 13, 00));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21));
      }
    },

    "step": {
      "increments time by one hour": function(tier) {
        var date = utc(2011, 08, 02, 22, 00, 00);
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 02, 23, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 01, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 02, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 03, 00));
      },
      "does not round the specified date": function(tier) {
        assert.deepEqual(tier.step(utc(2011, 08, 02, 12, 21, 23)), utc(2011, 08, 02, 13, 21, 23));
      },
      "does not modify the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 00, 00);
        assert.deepEqual(tier.step(date), utc(2011, 08, 02, 13, 00));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 00));
      }
    }
  },

  "day": {
    topic: tiers[864e5],
    "has the key 864e5": function(tier) {
      assert.strictEqual(tier.key, 864e5);
    },
    "next is the one-hour tier": function(tier) {
      assert.equal(tier.next, tiers[36e5]);
    },
    "size is 24": function(tier) {
      assert.strictEqual(tier.size(), 24);
    },

    "floor": {
      "rounds down to days": function(tier) {
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 00, 00, 00)), utc(2011, 08, 02, 00, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 00, 00, 01)), utc(2011, 08, 02, 00, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 02, 00, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 02, 23, 59, 59)), utc(2011, 08, 02, 00, 00));
        assert.deepEqual(tier.floor(utc(2011, 08, 03, 00, 00, 00)), utc(2011, 08, 03, 00, 00));
      },
      "does not modify the passed-in date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21);
        assert.deepEqual(tier.floor(date), utc(2011, 08, 02, 00, 00));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21));
      }
    },

    "ceil": {
      "rounds up to days": function(tier) {
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 00, 00, 00)), utc(2011, 08, 02, 00, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 00, 00, 01)), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 12, 21, 00)), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 02, 23, 59, 59)), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(tier.ceil(utc(2011, 08, 03, 00, 00, 00)), utc(2011, 08, 03, 00, 00));
      },
      "does not modified the specified date": function(tier) {
        var date = utc(2011, 08, 02, 12, 21, 00);
        assert.deepEqual(tier.ceil(date), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(date, utc(2011, 08, 02, 12, 21));
      }
    },

    "step": {
      "increments time by one day": function(tier) {
        var date = utc(2011, 08, 02, 00, 00, 00);
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 04, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 05, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 06, 00, 00));
        assert.deepEqual(date = tier.step(date), utc(2011, 08, 07, 00, 00));
      },
      "does not round the specified date": function(tier) {
        assert.deepEqual(tier.step(utc(2011, 08, 02, 12, 21, 23)), utc(2011, 08, 03, 12, 21, 23));
      },
      "does not modify the specified date": function(tier) {
        var date = utc(2011, 08, 02, 00, 00, 00);
        assert.deepEqual(tier.step(date), utc(2011, 08, 03, 00, 00));
        assert.deepEqual(date, utc(2011, 08, 02, 00, 00));
      }
    }
  }

});

function utc(year, month, day, hours, minutes, seconds) {
  return new Date(Date.UTC(year, month, day, hours || 0, minutes || 0, seconds || 0));
}

suite.export(module);

var vows        = require("vows"),
    assert      = require("assert"),
    test_helper = require('./test_helper'),
    metalog     = require("../lib/cube/metalog"),
    authentication = require("../lib/cube/authentication");

var suite = vows.describe("authentication");
suite.options.error = true;

//
// Test Macros
//

function successful_auth(req) {
  return function(authenticator){
    var that = this;
    that.req = req;
    authenticator.check(req,
                        function(arg){ that.callback(null, arg); },
                        function(arg){ that.callback(new Error("Auth failed but should have succeeded")); });
  };
}

function failed_auth(req) {
  return function(authenticator){
    var that = this;
    that.req = req;
    authenticator.check(req,
                        function(arg){ that.callback("Auth succeeded but should have failed"); },
                        function(arg){ that.callback(null, arg); } );
  };
}

// as I'm sure you know: boss_hogg and luke have write access (luke doubly so);
// roscoe can't do much but sit around; and it's as if coy & vance never existed
var test_users = [
  { _id: "boss_hogg", tokens: [{ uid: 'boss_hogg_tok', admin: true,  hashed_secret: "$2a$10$3u.CU4pJLnPDM7VwhJtbyuLwGBiOwpQ42q0wFQEDoJZtirAgIrBI6"}] },
  { _id: "luke",      tokens: [{ uid: 'luke_tok',      admin: true,  hashed_secret: "$2a$10$K5NpLr3qrhxsUBW0iCw8iegQzgEINdWDk2n1BrTYe1x1Ay4dU2PlG"},
                               { uid: 'luke_2_tok',    admin: true,  hashed_secret: "$2a$10$0I6KVPSzUIXdlxdY7qPTF.dde4tjPGRahYcja96Fz6ZaakEfdnNGO"}] },
  { _id: "roscoe",    tokens: [{ uid: 'roscoe_tok',    admin: false, hashed_secret: "$2a$10$BKIqJukrlFtbjFeeLCnEvOwHdLMLDt61iyfMRLiEf9lNeWKD.djrm"}] },
  { _id: "vance",     tokens: [] }
];
var dummy_token = "token_in_cookie";

function dummy_request(username, token){
  return({ headers: { cookie: authentication.gen_cookie("_cube_session", username+"_tok", token || dummy_token) } });
};

suite.addBatch(test_helper.batch({
  mongo_cookie: {
    topic: function(test_db){ test_db.using_objects("test_users", test_users, this) },
    "": {
      topic: function(test_db){
        return authentication.authenticator("mongo_cookie", test_db.db, { collection: "test_users" }); },
      "authenticates": {
        "users with good tokens": {
          topic: successful_auth(dummy_request("boss_hogg")),
          '': function(result){ assert.deepEqual(this.req.authorized, { uid: 'boss_hogg_tok', admin: true }); }
        },
        "users with tokens, even if there are many": {
          "a": {
            topic: successful_auth(dummy_request("luke")),
            '': function(result){ assert.deepEqual(this.req.authorized, { uid: 'luke_tok', admin: true }); }
          },
          "b": {
            topic: successful_auth(dummy_request("luke_2")),
            '': function(result){ assert.deepEqual(this.req.authorized, { uid: 'luke_2_tok', admin: true }); }
          }
        }
      },
      "request.authorized": {
        "": {
          topic: successful_auth(dummy_request("boss_hogg")),
          'is stapled to the request object': function(result){
            assert.isObject(this.req.authorized);
            assert.deepEqual(this.req.authorized, { uid: 'boss_hogg_tok', admin: true });
          },
          'is returned as callback param': function(result){
            assert.deepEqual(result, { uid: 'boss_hogg_tok', admin: true });
          },
        },
        "user with write access": {
          topic: successful_auth(dummy_request("boss_hogg")),
          'request.authorized.admin is true': function(result){
            assert.isTrue(this.req.authorized.admin);
          }
        },
        "user with read-only access": {
          topic: successful_auth(dummy_request("roscoe")),
          'authenticates': function(result){
            assert.deepEqual(this.req.authorized, { uid: 'roscoe_tok', admin: false }); },
          'request.authorized.admin is false': function(result){
            assert.isFalse(this.req.authorized.admin);
          }
        }
      },
      "does not allow": {
        "bad token": {
          topic: failed_auth(dummy_request("boss_hogg", "bad_token")),
          "invokes auth_no callback with reason": function(reason){
            assert.equal(reason, 'bad_token');
          },
          "does not authorize request": function(reason){ assert.isUndefined(this.req.authorized); }
        },
        "no token in request": {
          topic: failed_auth({ headers: { cookie: "" } }),
          "invokes auth_no callback with reason": function(reason){
            assert.equal(reason, 'no_token_in_request');
          },
          "does not authorize request": function(reason){ assert.isUndefined(this.req.authorized); }
        },
        "user there, no auth record": {
          topic: failed_auth(dummy_request("vance")),
          "invokes auth_no callback with reason": function(reason){
            assert.equal(reason, 'missing_user');
          },
          "does not authorize request": function(reason){ assert.isUndefined(this.req.authorized); }
        },
        "missing user": {
          topic: failed_auth(dummy_request("coy")),
          "invokes auth_no callback with reason": function(reason){
            assert.equal(reason, 'missing_user');
          },
          "does not authorize request": function(reason){ assert.isUndefined(this.req.authorized); }
        }
      }
    }
  },

  "allow_all": {
    topic: function(test_db){
      return authentication.authenticator("allow_all"); },
    "calls the auth_ok callback immediately" : {
      topic: successful_auth({type: "allow_all auth"}),
      'decorates the request object': function(result){
        assert.isObject(this.req.authorized);
      },
      'returns the authorization hash': function(result){
        assert.deepEqual(result,              { admin: true });
      },
      'authorizes writes': function(result){
        assert.deepEqual(this.req.authorized, { admin: true });
      }
    }
  },

  "read_only": {
    topic: function(test_db){
      return authentication.authenticator("read_only"); },
    "calls the auth_ok callback immediately" : {
      topic: successful_auth({type: "read_only auth"}),
      'decorates the request object': function(result){
        assert.isObject(this.req.authorized);
      },
      'returns the authorization hash': function(result){
        assert.deepEqual(result,              { admin: false });
      },
      'authorizes writes': function(result){
        assert.deepEqual(this.req.authorized, { admin: false });
      }
    }
  }

}));

suite.export(module);

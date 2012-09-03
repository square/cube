'use strict';

var options = require("../config/cube").include('warmer'),
    cube    = require("../"),
    warmer  = cube.warmer(options);

warmer.start();

var express = require('express');
var router = express.Router();
var db = require("../db.js");
var log = require('../helpers/log.js');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'grid.' });
});



module.exports = router;

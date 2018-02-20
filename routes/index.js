var express = require('express');
var router = express.Router();
var db = require("../db.js");
var log = require('../helpers/log.js');
var bookie = require('./bookie.js')

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'grid' });
});

/* TEST post request for running ad hoc analysis */
router.post('/test_code', function(req, res, next) {
	output_html=bookie.testing()
	res.send(output_html)
})

module.exports = router;

var express = require('express');
var router = express.Router();
var db = require("../db.js");
var log = require('../helpers/log.js');
var bookie = require('./bookie.js')

// Opening functions

bookie.kickoff()

// Routes

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'grid' });
});

/* TEST post request for running ad hoc analysis */
router.post('/test_code', function(req, res, next) {
	//bookie.collection(function(response_html) {
	//	res.send(response_html)
	//})
})

module.exports = router;

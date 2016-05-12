var MAX_EVENTS = 3600; // one hour

var graphData = {};

var keys = [ "users", "requests", "errors", "ads1", "ads2", "ads3" ];
var keymap = {
	'SESS' : 'users',
	'REQ' : 'requests',
	'ERR' : 'errors',
	'AD#1' : 'ads1',
	'AD#2' : 'ads2',
	'AD#3' : 'ads3'
};

var messages = [];
var MAX_MSGS = 100;

var handleNewData = function(newData) {
	messages.push(newData);
	if(messages.length >= MAX_MSGS)
		messages.shift();
	
	var messagesArea = document.getElementById('messages');
	messagesArea.innerHTML = messages.join('\n');
	messagesArea.scrollTop = messagesArea.scrollHeight;

	if (newData.indexOf(':(') != 13)
		return;

	var time = new Date(parseInt(newData.substring(0, 13)));
	var timeKey = time.getTime();
	var data = newData.substring(15, newData.length - 1);
	var stats = data.split(',');
	stats.forEach(function(s) {
		var statKV = s.trim().split('->');
		var statKey = keymap[statKV[0].trim()];
		var statVal = parseInt(statKV[1].trim());

		if (!graphData.hasOwnProperty(timeKey))
			graphData[timeKey] = {};
		if (graphData[timeKey].hasOwnProperty(statKey))
			graphData[timeKey][statKey] = graphData[timeKey][statKey] + statVal;
		else
			graphData[timeKey][statKey] = statVal;
		graphData[timeKey].TIME = time;
	});

	delete graphData[timeKey - MAX_EVENTS * 1000];
};//handleNewData

var timeRangeMins = 2;

var getGraphData = function(name) {
	var gData = [];
	
	var lowtime = Math.floor(new Date().getTime() / 1000) * 1000 - timeRangeMins * 60 * 1000;

	for (var d in graphData) {
		if (graphData.hasOwnProperty(d) && graphData[d].TIME.getTime() >= lowtime) {
			if (graphData[d].hasOwnProperty(name)) {
				gData.push({
					time : graphData[d].TIME,
					value : +graphData[d][name]
				});
			} else
				gData.push({
					time : graphData[d].TIME,
					value : 0
				});
		}
	}
	return gData.sort(function(a, b) {
		return a.time.getTime() - b.time.getTime();
	});
};//getGraphData

// D3:
var margin = { top : 20, right : 80, bottom : 30, left : 50 }, 
	width = 700 - margin.left - margin.right, height = 320 - margin.top	- margin.bottom;

var usrx = d3.time.scale().range([ 0, width ]);
var usry = d3.scale.linear().range([ height, 0 ]);

var reqy = d3.scale.linear().range([ height, 0 ]);

var color = d3.scale.category10();
color.domain(keys);

var xAxisUsr = d3.svg.axis().scale(usrx).orient("bottom");
var yAxisUsr = d3.svg.axis().scale(usry).orient("left");

var xAxisReq = d3.svg.axis().scale(usrx).orient("bottom");
var yAxisReq = d3.svg.axis().scale(reqy).orient("left");

var usrline = d3.svg.line().interpolate("basis")
	.x(function(d) { return usrx(d.time); })
	.y(function(d) { return usry(d.value); });
var reqline = d3.svg.line().interpolate("basis")
	.x(function(d) { return usrx(d.time); })
	.y(function(d) { return reqy(d.value); });

var usersvg = d3.select("#usergraph")
	.append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height",	height + margin.top + margin.bottom)
	.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
var reqsvg = d3.select("#reqgraph")
	.append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
	.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

// LEGEND
var timeNow = Math.floor(new Date().getTime() / 1000) * 1000;
for (var i = 1; i < color.domain().length; i++) {
	var func = d3.svg.line()
		.interpolate("basis")
		.x(function(d) { return d.x; })
		.y(function(d) { return d.y; });
	var dval = func([ { x : width - 35,	y : (i - 1) * 15 + 1 }, 
	                  {	x : width - 5,	y : (i - 1) * 15 + 1 } ]);
	reqsvg.append("path")
		.attr("class", "line")
		.attr("d", dval)
		.style("stroke", color(keys[i]));
	reqsvg.append("text")
		.attr("transform", function(d) { return "translate(" + width + "," + ((i - 1) * 15) + ")"; })
		.attr("x", 3).attr("dy", ".35em").text(function(d) {
		return keys[i];
	});
}

var xAxisSelUsr, yAxisSelUsr, metricSelUsr, xAxisSelReq, yAxisSelReq, metricSelReq;

var updateGraphs = function() {
	var timeNow = Math.floor(new Date().getTime() / 1000) * 1000;

	var reqmetrics = color.domain().map(function(name) {
		return {
			name : name,
			values : getGraphData(name)
		};
	});
	var usrmetrics = [];
	usrmetrics[0] = reqmetrics[0];
	reqmetrics.shift();

	usrx.domain([ new Date(timeNow - timeRangeMins * 60 * 1000), new Date(timeNow) ]);
	var usrmax = d3.max(usrmetrics, function(m) {
		return d3.max(m.values, function(v) { return v.value; }) 
	});
	if(!usrmax || usrmax == 0)
		usrmax = 5;
	usry.domain([ 0,  usrmax]);
	var reqmax = d3.max(reqmetrics, function(m) {
		return d3.max(m.values, function(v) { return v.value; })
	});
	if(!reqmax || reqmax == 0)
		reqmax = 5;
	reqy.domain([ 0,  reqmax]);

	if (xAxisSelUsr)
		xAxisSelUsr.remove();
	if (yAxisSelUsr)
		yAxisSelUsr.remove();
	if (xAxisSelReq)
		xAxisSelReq.remove();
	if (yAxisSelReq)
		yAxisSelReq.remove();

	xAxisSelUsr = usersvg.append("g");
	xAxisSelUsr.attr("class", "x axis")
		.attr("transform", "translate(0," + height + ")")
		.call(xAxisUsr);
	yAxisSelUsr = usersvg.append("g");
	yAxisSelUsr.attr("class", "y axis")
		.call(yAxisUsr)
		.append("text")
			.attr("transform", "rotate(-90)")
			.attr("y", 6)
			.attr("dy", ".7em")
			.style("text-anchor", "end")
			.text("Active user sessions");

	xAxisSelReq = reqsvg.append("g");
	xAxisSelReq
		.attr("class", "x axis")
		.attr("transform", "translate(0," + height + ")")
		.call(xAxisReq);
	yAxisSelReq = reqsvg.append("g");
	yAxisSelReq
		.attr("class", "y axis")
		.call(yAxisReq)
		.append("text")
			.attr("transform", "rotate(-90)")
			.attr("y", 6)
			.attr("dy", ".7em")
			.style("text-anchor", "end")
			.text("Number of events per second");

	if (metricSelUsr)
		metricSelUsr.remove();
	if (metricSelReq)
		metricSelReq.remove();

	metricSelUsr = usersvg.selectAll(".usrmetric");
	var usrmetric = metricSelUsr.data(usrmetrics)
		.enter().append("g")
		.attr("class", "usrmetric");
	metricSelReq = reqsvg.selectAll(".reqmetric");
	var reqmetric = metricSelReq.data(reqmetrics)
		.enter().append("g")
		.attr("class", "reqmetric");

	usrmetric.append("path")
		.attr("class", "line")
		.attr("d", function(d) { return usrline(d.values); })
			.style("stroke", function(d) { return color(d.name); });
	reqmetric.append("path")
		.attr("class", "line")
		.attr("d", function(d) { return reqline(d.values); })
			.style("stroke", function(d) { return color(d.name); });
};// updateGraphs

var setRange = function(range) {
	if(range == timeRangeMins)
		return;
	timeRangeMins = range;
	updateGraphs();
};

updateGraphs();

var runInterval;
var running = true;
var webSocket;

var startReceiving = function() {
	webSocket = new WebSocket('ws://' + window.document.location.host + '/WebStatsDashboard/WebStatsEndpoint');
	webSocket.onerror = function(event) {
		alert("An error has occurred during communication with the server.");
	};
	webSocket.onopen = function(event) {
	};
	webSocket.onclose = function() { stop(); };
	webSocket.onmessage = function(event) {
		var eventData = event.data;

		handleNewData(eventData);
	};
	
	runInterval = setInterval(updateGraphs, 1000);

	running = true;
};//startReceiving

var stopReceiving = function() {
	webSocket.close();
	clearInterval(runInterval);
	running = false;
};

startReceiving();

var stop = function() {
	stopReceiving();
	$('#runningbtn').attr('class', 'btn btn-danger');
	$('#tsp').text('Start');
};

var start = function() {
	startReceiving();
	$('#runningbtn').attr('class', 'btn btn-danger active');
	$('#tsp').text('Stop');	
};

var toggleRunning = function() {
	if(running)
	{
		stop();
	}
	else
	{	
		start();
	}
};//toggleRunning

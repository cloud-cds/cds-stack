window.onload = function() {
	graph();
};


function graph() {
	var graphWidth = Math.floor($('#graph-wrapper').width()) - 60;
	// console.log(graphWidth);
	$("#graphdiv").width(graphWidth);
	$("#graphdiv").height(graphWidth * .3625);
	var ctx = document.getElementById("graphdiv");

	var sin = [],
		cos = [];

	for (var i = 0; i < 14; i += 0.5) {
		sin.push([i, Math.sin(i)]);
		cos.push([i, Math.cos(i)]);
	}

	var plot = $.plot("#graphdiv", [
		{ data: sin, label: "sin(x)"},
		{ data: cos, label: "cos(x)"}
	], {
		series: {
			lines: {
				show: true
			},
			points: {
				show: true
			}
		},
		grid: {
			hoverable: true,
			clickable: true
		},
		yaxis: {
			min: -1.2,
			max: 1.2
		}
	});
}
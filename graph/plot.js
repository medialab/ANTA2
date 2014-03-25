var margin = {top: 30, right: 30, bottom: 40, left: 40},
    width = 600 - margin.left - margin.right,
    height = 600 - margin.top - margin.bottom;

var x = d3.scale.linear().range([0, width]).domain([0, 2]);

var y = d3.scale.linear().range([0, height]).domain([1, -1]);

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom");

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left");

var svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

d3.json("data/gl-enb2.json", function(error, data) {
    data.gl.terms.forEach(function(d) {
        //console.log(d);
    });

// Locality area
svg.append("rect")
    .attr("x", 0)
    .attr("y", 0)
    .attr("width", width / 5)
    .attr("height", height)
    .attr("fill", "#E6E6E6");
svg.append("text")
    .attr("class", "label")
    .attr("x", width / 10)
    .attr("y", 15)
    .attr("text-anchor", "middle")
    .text("Nowhere");

svg.append("rect")
    .attr("x", width / 4)
    .attr("y", 0)
    .attr("width", width / 2)
    .attr("height", height)
    .attr("fill", "#CFCFCF");
svg.append("text")
    .attr("class", "label")
    .attr("x", width / 2)
    .attr("y", 15)
    .attr("text-anchor", "middle")
    .text("Somewhere");

svg.append("rect")
    .attr("x", width * 4 / 5)
    .attr("y", 0)
    .attr("width", width / 5)
    .attr("height", height)
    .attr("fill", "#E6E6E6");
svg.append("text")
    .attr("class", "label")
    .attr("x", width * 9 / 10)
    .attr("y", 15)
    .attr("text-anchor", "middle")
    .text("Everywhere");

// X Y Axis
svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height / 2 + ")")
    .call(xAxis)
    .append("text")
    .attr("class", "label")
    .attr("x", width - 6)
    .attr("y", -6)
    .style("text-anchor", "end")
    .text("Locality");

svg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
    .append("text")
    .attr("class", "label")
    .attr("transform", "rotate(-90)")
    .attr("x", -6)
    .attr("y", 6)
    .attr("dy", ".71em")
    .style("text-anchor", "end")
    .text("Genericity");


// Possible values rect
svg.append("line")
    .attr("x1", 0)
    .attr("y1", height / 2)
    .attr("x2", width / 2)
    .attr("y2", 0)
    .attr("stroke-width", 1)
    .attr("stroke", "#303030");
svg.append("line")
    .attr("x1", width / 2)
    .attr("y1", 0)
    .attr("x2", width)
    .attr("y2", height / 2)
    .attr("stroke-width", 1)
    .attr("stroke", "#303030");
svg.append("line")
    .attr("x1", width)
    .attr("y1", height / 2)
    .attr("x2", width / 2)
    .attr("y2", height)
    .attr("stroke-width", 1)
    .attr("stroke", "#303030");
svg.append("line")
    .attr("x1", width / 2)
    .attr("y1", height)
    .attr("x2", 0)
    .attr("y2", height / 2)
    .attr("stroke-width", 1)
    .attr("stroke", "#303030");

// S V Label
svg.append("text")
    .attr("class", "label")
    .attr("x", width * 0.225)
    .attr("y", height / 4)
    .style("text-anchor", "middle")
    .text("S");
svg.append("text")
    .attr("class", "label")
    .attr("x", width * 0.225)
    .attr("y", height * 3 / 4)
    .attr("dy", ".71em")
    .style("text-anchor", "middle")
    .text("V");

// Dot
svg.selectAll(".dot")
    .data(data.gl.terms)
    .enter().append("circle")
    .attr("class", "dot")
    .attr("cx", function(d) { return x(d.L); })
    .attr("cy", function(d) { return y(d.G); })
    .attr("r", 3.5)
    .style("fill", "#0033CC");

/**
svg.selectAll("text")
    .data(data.gl.terms)
    .enter().append("text")
    .text(function(d) { return d.term;})
    .attr("x", function(d) { return x(d.L) + 6; })
    .attr("y", function(d) { return y(d.G) + 2; })
    .attr("font-family", "sans-serif")
    .attr("font-size", "11px")
    .attr("fill", "#303030");
**/
});
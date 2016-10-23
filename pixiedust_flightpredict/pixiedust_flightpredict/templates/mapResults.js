{%import "commonExecuteCallback.js" as commons%}

var True=true;
var False=false;
var graph = {"nodes": {{graphNodesJson}},"links":{{graphLinksJson}}};
var prefix = '{{prefix}}';
var parentBox = d3.select("#map" + prefix).node().getBoundingClientRect();
debugger;
var w = parentBox.width;
var h = parentBox.height;

var linksByOrigin = {};
var countByAirport = {};
var locationByAirport = {};
var positions = [];
var d3_geo_radians = Math.PI / 180;
d3.geo.azimuthal = function() {
  var mode = "orthographic", // or stereographic, gnomonic, equidistant or equalarea
      origin,
      scale = 200,
      translate = [480, 250],
      x0,
      y0,
      cy0,
      sy0;

  function azimuthal(coordinates) {
    var x1 = coordinates[0] * d3_geo_radians - x0,
        y1 = coordinates[1] * d3_geo_radians,
        cx1 = Math.cos(x1),
        sx1 = Math.sin(x1),
        cy1 = Math.cos(y1),
        sy1 = Math.sin(y1),
        cc = mode !== "orthographic" ? sy0 * sy1 + cy0 * cy1 * cx1 : null,
        c,
        k = mode === "stereographic" ? 1 / (1 + cc)
          : mode === "gnomonic" ? 1 / cc
          : mode === "equidistant" ? (c = Math.acos(cc), c ? c / Math.sin(c) : 0)
          : mode === "equalarea" ? Math.sqrt(2 / (1 + cc))
          : 1,
        x = k * cy1 * sx1,
        y = k * (sy0 * cy1 * cx1 - cy0 * sy1);
    return [
      scale * x + translate[0],
      scale * y + translate[1]
    ];
  }

  azimuthal.invert = function(coordinates) {
    var x = (coordinates[0] - translate[0]) / scale,
        y = (coordinates[1] - translate[1]) / scale,
        p = Math.sqrt(x * x + y * y),
        c = mode === "stereographic" ? 2 * Math.atan(p)
          : mode === "gnomonic" ? Math.atan(p)
          : mode === "equidistant" ? p
          : mode === "equalarea" ? 2 * Math.asin(.5 * p)
          : Math.asin(p),
        sc = Math.sin(c),
        cc = Math.cos(c);
    return [
      (x0 + Math.atan2(x * sc, p * cy0 * cc + y * sy0 * sc)) / d3_geo_radians,
      Math.asin(cc * sy0 - (p ? (y * sc * cy0) / p : 0)) / d3_geo_radians
    ];
  };

  azimuthal.mode = function(x) {
    if (!arguments.length) return mode;
    mode = x + "";
    return azimuthal;
  };

  azimuthal.origin = function(x) {
    if (!arguments.length) return origin;
    origin = x;
    x0 = origin[0] * d3_geo_radians;
    y0 = origin[1] * d3_geo_radians;
    cy0 = Math.cos(y0);
    sy0 = Math.sin(y0);
    return azimuthal;
  };

  azimuthal.scale = function(x) {
    if (!arguments.length) return scale;
    scale = +x;
    return azimuthal;
  };

  azimuthal.translate = function(x) {
    if (!arguments.length) return translate;
    translate = [+x[0], +x[1]];
    return azimuthal;
  };

  return azimuthal.origin([0, 0]);
};

var projection = d3.geo.azimuthal()
    .mode("equidistant")
    .origin([-98, 38])
    .scale(1400)
    .translate([w/2, h/2]);

var path = d3.geo.path().projection(projection);

var svg = d3.select("#map" + prefix).insert("svg:svg", "h2").attr("width", w).attr("height", h);

var states = svg.append("svg:g").attr("id", "states")
var circles = svg.append("svg:g").attr("id", "circles");
var cells = svg.append("svg:g").attr("id", "cells");

/*var arcGraticule = d3.geo.graticule().majorExtent()
    .source(function(d) { return locationByAirport[d.source]; })
    .target(function(d) { return locationByAirport[d.target]; });*/
var arc = d3.geo.greatArc()
    .source(function(d) { return locationByAirport[d.source]; })
    .target(function(d) { return locationByAirport[d.target]; });

// Draw US map.
d3.json("https://mbostock.github.io/d3/talk/20111116/us-states.json", function(collection) {
    states.selectAll("path")
    .data(collection.features)
    .enter().append("svg:path")
        .attr("d", path);
    }
);

// Parse links
graph.links.forEach(function(link) {
    var links = linksByOrigin[link.src] || (linksByOrigin[link.src] = []);
    links.push({ source: link.src, target: link.dst });
    countByAirport[link.src] = (countByAirport[link.src] || 0) + 1;
    countByAirport[link.dst] = (countByAirport[link.dst] || 0) + 1;
});
        
var keys=[]
for (key in graph.nodes){
    var v = graph.nodes[key];
    var loc=[+v.longitude,+v.latitude];
    locationByAirport[key] = loc;
    positions.push(projection(loc));
    keys.push(v)
}
var g = cells.selectAll("g#cells").data(keys).enter().append("svg:g").attr("dep", function(d){return d.id})
function deselect(e) {
    $('.pop').slideFadeToggle(function() {
        e.removeClass('selected');
    });    
}

$('#flightBadgeClose').click(function(){
    deselect($(this));
});

$.fn.slideFadeToggle = function(easing, callback) {
  return this.animate({ opacity: 'toggle', height: 'toggle' }, 'fast', easing, callback);
};
          
g.selectAll("path.arc")
    .data(function(d) { 
        return linksByOrigin[d.id] || []; 
    })
    .enter().append("svg:path")
    .attr("dep", function(d){return d.id})
    .attr("class", "arc")
    .attr("d", function(d) {
        return path(arc(d)); 
    })
    .on("click", function(d,i){
        console.log("d3 mouse", d3.mouse(this))
        console.log("pageY", d3.event.pageY)
        console.log("screenY", d3.event.screenY)
        console.log("offsetY", d3.event.offsetY)
        if($(this).hasClass('selected')) {
            deselect($(this));               
        } else {
            $(this).addClass('selected');
            var d3EventX = d3.event.offsetX;
            var d3EventY = d3.event.offsetY;
            if (!window.Pixiedust) window.Pixiedust={}
            var $$command = "from pixiedust_flightpredict.running.flightHistory import *" +
                "\nprint(getBadgeHtml('" + d.source + "', '" + d.target + "'))";

            {% call(results) commons.ipython_execute("$$command", prefix) %}
                $('#flightBadgeBody').html({{results}})
                $('#flightBadge')
                    .css({left:(window.pageXOffset + d3EventX + 100)+'px', top: (window.pageYOffset + d3EventY)+'px',})
                    .slideFadeToggle()
            {% endcall %}
        }
    })

var selectedAirport=null;
function toggleAirportSelection(airport){
    if ( airport ){
        el = d3.select("g[dep='" + airport + "']");
        el.classed("showFlights", !el.classed("showFlights"))
    }
}
circles.selectAll("circle")
    .data(keys)
    .enter()
    .append("svg:circle")
    .attr("airport", function(d){return d.id})
    .attr("class", function(d){ return (linksByOrigin[d.id]?"origin":"")})
    .attr("cx", function(d, i) {return positions[i][0]; })
    .attr("cy", function(d, i) {return positions[i][1];})
    .attr("r", function(d, i) { return Math.max(10, Math.sqrt(countByAirport[d.id])); })
    .on("mouseover", function(d, i) { 
        d3.select("#airport" + prefix).text(d.name + "(" + d.id + ")" ); 
    })
    .on("mouseout", function(d, i) { 
        d3.select("#airport" + prefix).text(""); 
    })
    .on("click", function(d, i) {
        if (d3.select(this).classed("origin") ){
            toggleAirportSelection(selectedAirport);
            if (selectedAirport !== d.id ){            
                selectedAirport = d.id;
                toggleAirportSelection(selectedAirport);
            }else{
                selectedAirport = null;
            }
        }
    })
    .sort(function(a, b) { return (countByAirport[b.id] - countByAirport[a.id]); })
/*
 * openark_lchart.js
 * A line chart javascript implementation. Currently can read google line chart URLs (partial feature list).
 * Uses VML on Internet Explorer, and HTML <canvas> on all other browsers.
 * 
 * 
 * Released under the BSD license
 * 
 * Copyright (c) 2009-2010, Shlomi Noach
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *  * Neither the name of the organization nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

openark_lchart = function(container, options) {
	if (container.style.width == '')
		this.canvas_width = options.width;
	else
		this.canvas_width = container.style.width;
	if (container.style.height == '')
		this.canvas_height = options.height;
	else
		this.canvas_height = container.style.height;
	this.title_height = 0;
	this.chart_title = '';
	this.x_axis_values_height = 20;
	this.y_axis_values_width = 50;
	this.y_axis_tick_values = [];
	this.y_axis_tick_positions = [];
	this.x_axis_grid_positions = [];
	this.x_axis_label_positions = [];
	this.x_axis_labels = [];
	this.y_axis_min = 0;
	this.y_axis_max = 0;
	this.multi_series = [];
	this.multi_series_dot_positions = [];
	this.series_labels = [];
	this.series_colors = openark_lchart.series_colors;
	
	this.container = container;
	
	this.isIE = false;
	this.current_color = null;
	
	this.recalc();
	
	return this;
};


openark_lchart.title_font_size = 10;
openark_lchart.title_color = '#505050';
openark_lchart.axis_color = '#707070';
openark_lchart.axis_font_size = 8;
openark_lchart.legend_font_size = 9;
openark_lchart.legend_color = '#606060';
openark_lchart.series_line_width = 1.5;
openark_lchart.grid_color = '#e4e4e4';
openark_lchart.grid_thick_color = '#c8c8c8';
openark_lchart.series_colors = ["#ff0000", "#ff8c00", "#4682b4", "#9acd32", "#dc143c", "#9932cc", "#ffd700", "#191970", "#7fffd4", "#808080", "#dda0dd"];
openark_lchart.google_simple_format_scheme = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";



openark_lchart.prototype.recalc = function() {
	this.chart_width = this.canvas_width - this.y_axis_values_width;
	this.chart_height = this.canvas_height - (this.x_axis_values_height + this.title_height);
	this.chart_origin_x = this.canvas_width - this.chart_width;
	this.chart_origin_y = this.title_height + this.chart_height;
	
	// Calculate y-ticks:
	this.y_axis_tick_values = [];
	this.y_axis_tick_positions = [];
	if (this.y_axis_max <= this.y_axis_min)
	{
		return;
	}
    // Find tick nice round steps.
	max_steps = Math.floor(this.chart_height / (openark_lchart.axis_font_size * 1.6));
    round_steps_basis = [1, 2, 5];
    step_size = null;
    pow = 0;
    
    for (power = -4; power < 10 && !step_size; ++power) {
    	for (i = 0 ; i < round_steps_basis.length && !step_size; ++i)
    	{
    		round_step = round_steps_basis[i] * Math.pow(10, power);
        	if ((this.y_axis_max - this.y_axis_min)/round_step < max_steps) {
        		step_size = round_step;
        		pow = power;
        	}
    	}
    }
	var tick_value = step_size*Math.ceil(this.y_axis_min/step_size);
	while (tick_value <= this.y_axis_max)
	{
		var display_tick_value = (pow >= 0 ? tick_value : tick_value.toFixed(-pow));
		this.y_axis_tick_values.push(display_tick_value);
		var tick_value_ratio = (tick_value - this.y_axis_min)/(this.y_axis_max - this.y_axis_min);
		this.y_axis_tick_positions.push(Math.floor(this.chart_origin_y - tick_value_ratio*this.chart_height));
		tick_value += step_size;
	}
	
};


openark_lchart.prototype.create_graphics = function() {
	this.container.innerHTML = '';
	
	this.isIE = (/MSIE/.test(navigator.userAgent) && !window.opera);

	this.container.style.position = 'relative';
	this.container.style.color = ''+openark_lchart.axis_color;
	this.container.style.fontSize = ''+openark_lchart.axis_font_size+'pt';
	this.container.style.fontFamily = 'Helvetica,Verdana,Arial,sans-serif';

	if (this.isIE)
	{
		// Nothing to do here right now.
	}
	else
	{
		var canvas = document.createElement("canvas");
		canvas.setAttribute("width", this.canvas_width);
		canvas.setAttribute("height", this.canvas_height);
		
		this.canvas = canvas;
		this.container.appendChild(this.canvas);
	
		this.ctx = this.canvas.getContext('2d');
	}
};

openark_lchart.prototype.parse_url = function(url) {
	url = url.replace(/[+]/gi, " ");
	var params = {};
	
	var pos = url.indexOf("?");
	if (pos >= 0)
		url = url.substring(pos + 1);
	tokens = url.split("&");
	for (i = 0 ; i < tokens.length ; ++i)
	{
		param_tokens = tokens[i].split("=");
		if (param_tokens.length == 2)
		params[param_tokens[0]] = param_tokens[1];
	}
	return params;
};

openark_lchart.prototype.read_google_url = function(url) {
	params = this.parse_url(url);
	// title:
	this.title_height = 0;
	if (params["chtt"])
	{
		this.chart_title = params["chtt"];
		this.title_height = 20;
	}
	// labels:
	if (params["chdl"])
	{
		var tokens = params["chdl"].split("|");
		this.series_labels = tokens;
	}
	if (params["chco"])
	{
		var tokens = params["chco"].split(",");
		this.series_colors = new Array(tokens.length);
		for (i = 0; i < tokens.length ; ++i)
			this.series_colors[i] = "#"+tokens[i];
	}
	// parse y-axis range:
	var tokens = params["chxr"].split(",");
	if (tokens.length >= 3)
	{
		this.y_axis_min = parseFloat(tokens[1]);
		this.y_axis_max = parseFloat(tokens[2]);
	}
	// Enough data to rebuild chart dimensions.
	this.recalc();
	// x (vertical) grids:
	var tokens = params["chg"].split(",");
	if (tokens.length >= 6)
	{
		var x_axis_step_size = parseFloat(tokens[0]);
		var x_offset = parseFloat(tokens[4]);
		this.x_axis_grid_positions = [];
		for(i = 0, chart_x_pos = 0; chart_x_pos < this.chart_width; ++i)
		{
			chart_x_pos = (x_offset + i*x_axis_step_size) * this.chart_width / 100;
			if (chart_x_pos < this.chart_width)
			{
				this.x_axis_grid_positions.push(Math.floor(chart_x_pos + this.chart_origin_x));
			}
		}
	}
	// x axis label positions:
	var tokens = params["chxp"].split("|");
	for (axis = 0; axis < tokens.length ; ++axis)
	{
		var axis_tokens = tokens[axis].split(",");
		var axis_number = parseInt(axis_tokens[0]);
		if (axis_number == 0)
		{
			this.x_axis_label_positions = new Array(axis_tokens.length - 1);
			for (i = 1; i < axis_tokens.length; ++i)
			{
				var label_position = parseFloat(axis_tokens[i]) * this.chart_width / 100.0;
				this.x_axis_label_positions[i - 1] = Math.floor(label_position + this.chart_origin_x);
			}
		}
	}
	// x axis labels:
	var tokens = params["chxl"].split("|");
	// I'm doing a shortcut here. I'm expecting a single axis! This is because the chxl parameter is not trivial to parse.
	// The following will FAIL when more than one axis is provided!
	if (tokens[0] == '0:')
	{
		this.x_axis_labels = new Array(tokens.length - 1);
		for (i = 1; i < tokens.length; ++i)
		{
			this.x_axis_labels[i - 1] = tokens[i];
		}
	}
	if (params["chd"])
	{
		var chd = params["chd"];
		var data_format = null;
		var chd_format_token = chd.substring(0, 2);
		if (chd_format_token == "s:")
			data_format = "simple";
		if (data_format)
		{
			this.multi_series = [];
			this.multi_series_dot_positions = [];
		}
		if (data_format == "simple")
		{
			chd = chd.substring(2);
			var tokens = chd.split(",");
			this.multi_series = new Array(tokens.length);
			this.multi_series_dot_positions = new Array(tokens.length);
			for (series_index = 0; series_index < tokens.length ; ++series_index)
			{
				var series_encoded_data = tokens[series_index];
				
				var series = new Array(series_encoded_data.length);
				var series_dot_positions = new Array(series_encoded_data.length);
				for (i = 0 ; i < series_encoded_data.length ; ++i)
				{
					var series_encoded_current_data = series_encoded_data.charAt(i);
					if (series_encoded_current_data == '_')
					{
						series[i] = null;
						series_dot_positions[i] = null;
					}
					else
					{
						var x_value_scale_ratio = openark_lchart.google_simple_format_scheme.indexOf(series_encoded_current_data)/61;
						var x_value = this.y_axis_min + x_value_scale_ratio*(this.y_axis_max-this.y_axis_min);
						series[i] = x_value;
						series_dot_positions[i] = Math.round(this.chart_origin_y - x_value_scale_ratio*this.chart_height);
					}
				}
				this.multi_series[series_index] = series;
				this.multi_series_dot_positions[series_index] = series_dot_positions;
			}
		}
	}

	this.redraw();
};

openark_lchart.prototype.redraw = function() {
	this.create_graphics();
	this.draw();
};

openark_lchart.prototype.draw = function() {
	// Title
	if (this.chart_title)
	{
		this.draw_text({
			text: this.chart_title, 
			left: 0, 
			top: 0, 
			width: this.canvas_width, 
			height: this.title_height, 
			text_align: 'center', 
			font_size: openark_lchart.title_font_size
		});
	}
	this.set_color(openark_lchart.grid_color);
	// y (horiz) grids:
	for (i = 0 ; i < this.y_axis_tick_positions.length ; ++i)
	{
		this.draw_line(this.chart_origin_x, this.y_axis_tick_positions[i], this.chart_origin_x + this.chart_width - 1, this.y_axis_tick_positions[i], 1);
	}
	// x (vertical) grids:
	for (i = 0 ; i < this.x_axis_grid_positions.length ; ++i)
	{
		if (this.x_axis_labels[i].replace(/ /gi, ""))
			this.set_color(openark_lchart.grid_thick_color);
		else
			this.set_color(openark_lchart.grid_color);
		this.draw_line(this.x_axis_grid_positions[i], this.chart_origin_y, this.x_axis_grid_positions[i], this.chart_origin_y - this.chart_height + 1, 1);
	}
	this.set_color(openark_lchart.axis_color);
	// x (vertical) ticks:
	for (i = 0 ; i < this.x_axis_label_positions.length ; ++i)
	{
		if (this.x_axis_labels[i])
		{
			// x-ticks:
			this.draw_line(this.x_axis_label_positions[i], this.chart_origin_y, this.x_axis_label_positions[i], this.chart_origin_y + 3, 1);
			
			// x-labels:
			if (this.x_axis_labels[i].replace(/ /gi, ""))
			{
				this.draw_text({
					text: ''+this.x_axis_labels[i], 
					left: this.x_axis_label_positions[i] - 25, 
					top: this.chart_origin_y + 5, 
					width: 50, 
					height: openark_lchart.axis_font_size, 
					text_align: 'center', 
					font_size: openark_lchart.axis_font_size
				});
			}
		}
	}
	// series:
	for (series = 0 ; series < this.multi_series_dot_positions.length ; ++series)
	{
		var paths = [];
		paths.push([]);
		this.set_color(this.series_colors[series]);
		var series_dot_positions = this.multi_series_dot_positions[series];
		for (i = 0 ; i < series_dot_positions.length ; ++i)
		{
			if (series_dot_positions[i] == null)
			{
				// New path due to null value
				paths.push([]);
			}
			else
			{
				var x_pos = Math.round(this.chart_origin_x + i*this.chart_width/(series_dot_positions.length-1));
				paths[paths.length-1].push({
					x: x_pos,
					y: series_dot_positions[i]
				});
			}
		}
		for (path = 0; path < paths.length; ++path)
			this.draw_line_path(paths[path], openark_lchart.series_line_width);
	}
	// axis lines
	this.set_color(openark_lchart.axis_color);
	this.draw_line(this.chart_origin_x, this.chart_origin_y, this.chart_origin_x, this.chart_origin_y - this.chart_height + 1, 1);
	this.draw_line(this.chart_origin_x, this.chart_origin_y, this.chart_origin_x + this.chart_width - 1, this.chart_origin_y, 1);
	var y_axis_labels = '';
	for (i = 0 ; i < this.y_axis_tick_positions.length ; ++i)
	{
		// y-ticks:
		this.draw_line(this.chart_origin_x, this.y_axis_tick_positions[i], this.chart_origin_x-3, this.y_axis_tick_positions[i], 1);
		// y-labels:
		this.draw_text({
			text: ''+this.y_axis_tick_values[i], 
			left: 0, 
			top: this.y_axis_tick_positions[i] - openark_lchart.axis_font_size + Math.floor(openark_lchart.axis_font_size/3), 
			width: this.y_axis_values_width - 5, 
			height: openark_lchart.axis_font_size, 
			text_align: 'right', 
			font_size: openark_lchart.axis_font_size
		}); 
	}
	// legend:
	if (this.series_labels && this.series_labels.length)
	{
		if (this.isIE)
		{
			// Since all drawings are done via VML and absolute positions, the 
			// entire container becomes dimensionless. We now force its dimensions:
			// We add a place holder for the "canvas", then add the legend div.
			var placeholder_div = document.createElement("div");
			placeholder_div.style.width = this.canvas_width;
			placeholder_div.style.height = this.canvas_height;
			this.container.appendChild(placeholder_div);
		}
		var legend_div = document.createElement("div");

		var legend_ul = document.createElement("ul");
		legend_ul.style.margin = 0;
		legend_ul.style.paddingLeft = this.chart_origin_x;
		for (i = 0 ; i < this.series_labels.length ; ++i)
		{
			var legend_li = document.createElement("li");
			legend_li.style.listStyleType = 'square';
			legend_li.style.color = this.series_colors[i];
			legend_li.style.fontSize = ''+openark_lchart.legend_font_size+'pt';
			legend_li.innerHTML = '<span style="color: '+openark_lchart.legend_color+'">'+this.series_labels[i]+'</span>';
			legend_ul.appendChild(legend_li);
		}
		legend_div.appendChild(legend_ul);
		this.container.appendChild(legend_div);
	}
};


openark_lchart.prototype.set_color = function(color) {
	this.current_color = color;
	if (!this.isIE)
	{
		this.ctx.strokeStyle = color;
	}
};


openark_lchart.prototype.draw_line = function(x0, y0, x1, y1, lineWidth) {
	if (this.isIE)
	{
		var line_element = document.createElement("v:line");
		line_element.setAttribute("from", ' '+x0+' '+y0+' ');
		line_element.setAttribute("to", ' '+x1+' '+y1+' ');
		line_element.setAttribute("strokecolor", ''+this.current_color);
		line_element.setAttribute("strokeweight", ''+lineWidth+'pt');
		this.container.appendChild(line_element);
	}
	else
	{
		this.ctx.lineWidth = lineWidth;
		this.ctx.strokeWidth = 0.5;
		this.ctx.beginPath();
		this.ctx.moveTo(x0+0.5, y0+0.5);
		this.ctx.lineTo(x1+0.5, y1+0.5);
		this.ctx.closePath();
		this.ctx.stroke();
	}
};


openark_lchart.prototype.draw_line_path = function(coordinates, lineWidth) {
	if (coordinates.length == 0)
		return;
	if (coordinates.length == 1)
	{
		this.draw_line(coordinates[0].x - 2, coordinates[0].y, coordinates[0].x + 2, coordinates[0].y, lineWidth*0.8);
		this.draw_line(coordinates[0].x, coordinates[0].y - 2, coordinates[0].x, coordinates[0].y + 2, lineWidth*0.8);
		return;
	}
	if (this.isIE)
	{
		var polyline_element = document.createElement("v:polyline");
		var linear_coordinates = new Array(coordinates.length*2);
		for (i = 0; i < coordinates.length; ++i)
		{
			linear_coordinates[i*2] = coordinates[i].x;
			linear_coordinates[i*2 + 1] = coordinates[i].y;
		}
		var points = linear_coordinates.join(',');;
		polyline_element.setAttribute("points", points);
		polyline_element.setAttribute("stroked", "true");
		polyline_element.setAttribute("filled", "false");
		polyline_element.setAttribute("strokecolor", ''+this.current_color);
		polyline_element.setAttribute("strokeweight", ''+lineWidth+'pt');
		this.container.appendChild(polyline_element);
	}
	else
	{
		this.ctx.lineWidth = lineWidth;
		this.ctx.strokeWidth = 0.5;
		this.ctx.beginPath();
		this.ctx.moveTo(coordinates[0].x+0.5, coordinates[0].y+0.5);
		for (i = 1; i < coordinates.length; ++i)
		{
			this.ctx.lineTo(coordinates[i].x+0.5, coordinates[i].y+0.5);
		}
		this.ctx.stroke();
	}
};


openark_lchart.prototype.draw_text = function(options) {
	var label_div = document.createElement("div");
	label_div.style.position = 'absolute';
	label_div.style.left = ''+options.left+'px';
	label_div.style.top = ''+options.top+'px';
	label_div.style.width = ''+options.width+'px';
	label_div.style.height = ''+options.height+'px';
	label_div.style.textAlign =''+options.text_align;
	label_div.style.verticalAlign ='top';
	if (options.font_size)
		label_div.style.fontSize = ''+options.font_size+'pt';
	label_div.innerHTML = options.text;
	this.container.appendChild(label_div);
};

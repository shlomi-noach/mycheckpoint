/*
 * g.Raphael 0.4 - Charting library, based on Raphaël
 *
 * Copyright (c) 2009 Dmitry Baranovskiy (http://g.raphaeljs.com)
 * Licensed under the MIT (http://www.opensource.org/licenses/mit-license.php) license.
 */
Raphael.fn.g.linechart = function (x, y, width, height, valuesx, valuesy, opts, labelsx) {
    function shrink(values, dim) {
        var k = values.length / dim,
            j = 0,
            l = k,
            sum = 0,
            res = [];
        while (j < values.length) {
            l--;
            if (l < 0) {
                sum += values[j] * (1 + l);
                res.push(sum / k);
                sum = values[j++] * -l;
                l += k;
            } else {
                sum += values[j++];
            }
        }
        return res;
    }
    opts = opts || {};
    if (!this.raphael.is(valuesx[0], "array")) {
        valuesx = [valuesx];
    }
    if (!this.raphael.is(valuesy[0], "array")) {
        valuesy = [valuesy];
    }
    function normalize_array(array, min_zero, max_100)
    {
	result = []
	for (var i = 0; i < array.length; i++) {
	    if (array[i] != null)
		result.push(array[i]);
	}
	if (min_zero)
	    result.push(0);
	if (max_100)
	    result.push(100);
	return result;
    }
    var allx = Array.prototype.concat.apply([], valuesx),
        ally = Array.prototype.concat.apply([], valuesy),
        xdim = this.g.snapEnds(Math.min.apply(Math, allx), Math.max.apply(Math, allx), valuesx[0].length - 1),
        minx = xdim.from,
        maxx = xdim.to,
        gutter = opts.gutter || 10,
        kx = (width - gutter * 2) / (maxx - minx),
	ally_normalized = normalize_array(ally, opts.min_zero, opts.max_100);
        ydim = this.g.snapEnds(Math.min.apply(Math, ally_normalized), Math.max.apply(Math, ally_normalized), valuesy[0].length - 1),
        miny = ydim.from,
        maxy = ydim.to,
        ky = (height - gutter * 2) / (maxy - miny),
        len = Math.max(valuesx[0].length, valuesy[0].length),
        symbol = opts.symbol || "",
        colors = opts.colors || Raphael.fn.g.colors,
	colors = ["#ff8c00", "#4682b4", "#9acd32", "#dc143c", "#9932cc", "#ffd700", "#191970", "#7fffd4", "#808080", "#dda0dd"];

        that = this,
        columns = null,
        dots = null,
        chart = this.set(),
        path = [];

    for (var i = 0, ii = valuesy.length; i < ii; i++) {
        len = Math.max(len, valuesy[i].length);
    }
    var shades = this.set();
    for (var i = 0, ii = valuesy.length; i < ii; i++) {
        if (opts.shade) {
            shades.push(this.path().attr({stroke: "none", fill: colors[i], opacity: opts.nostroke ? 1 : .3}));
        }
        if (valuesy[i].length > width - 2 * gutter) {
            valuesy[i] = shrink(valuesy[i], width - 2 * gutter);
            len = width - 2 * gutter;
        }
        if (valuesx[i] && valuesx[i].length > width - 2 * gutter) {
            valuesx[i] = shrink(valuesx[i], width - 2 * gutter);
        }
    }
    var axis = this.set();
    var lines = this.set(),
        symbols = this.set(),
        line;
    for (var i = 0, ii = valuesy.length; i < ii; i++) {
        if (!opts.nostroke) {
            lines.push(line = this.path().attr({
                stroke: colors[i],
                "stroke-width": opts.width || 2,
                "stroke-linejoin": "round",
                "stroke-linecap": "round",
                "stroke-dasharray": opts.dash || ""
            }));
        }
        var sym = this.raphael.is(symbol, "array") ? symbol[i] : symbol,
            symset = this.set();
        path = [];
	xpoints = [];
	var current_value_is_null = false;
	var last_value_is_null = false;
        for (var j = 0, jj = valuesy[i].length; j < jj; j++) {
	    current_value_is_null = (valuesy[i][j] == null);
            var X = x + gutter + ((valuesx[i] || valuesx[0])[j] - minx) * kx;
            var Y = y + height - gutter - (valuesy[i][j] - miny) * ky;
	    xpoints.push(X);
	    if (!current_value_is_null)
	    {
                (Raphael.is(sym, "array") ? sym[j] : sym) && symset.push(this.g[Raphael.fn.g.markers[this.raphael.is(sym, "array") ? sym[j] : sym]](X, Y, (opts.width || 2) * 3).attr({fill: colors[i], stroke: "none"}));
	    }
	    var should_draw_line = j && !last_value_is_null;
	    if (!current_value_is_null)
                path = path.concat([should_draw_line ? "L" : "M", X, Y]);
	    last_value_is_null = current_value_is_null;
        }
        symbols.push(symset);
        if (opts.shade) {
            shades[i].attr({path: path.concat(["L", X, y + height - gutter, "L",  x + gutter + ((valuesx[i] || valuesx[0])[0] - minx) * kx, y + height - gutter, "z"]).join(",")});
        }
        !opts.nostroke && line.attr({path: path.join(",")});
    }
    if (opts.axis) {
        var ax = (opts.axis + "").split(/[,\s]+/);
	// Bottom x-axis
        +ax[2] && axis.push(this.g.axis(x + gutter, y + height - gutter, width - 2 * gutter, minx, maxx, (labelsx? labelsx.length-1 : Math.floor((width - 2 * gutter) / 20)), 0, labelsx, "t", 2, height - 2 * gutter, xpoints));
	// Left y-axis
        +ax[3] && axis.push(this.g.axis(x + gutter, y + height - gutter, height - 2 * gutter, miny, maxy, opts.axisystep || Math.floor((height - 2 * gutter) / 20), 1, null, "t", 2, width - 2 * gutter, null));
    }
    function createColumns(f) {
        // unite Xs together
        var Xs = [];
        for (var i = 0, ii = valuesx.length; i < ii; i++) {
            Xs = Xs.concat(valuesx[i]);
        }
        Xs.sort();
        // remove duplicates
        var Xs2 = [],
            xs = [];
        for (var i = 0, ii = Xs.length; i < ii; i++) {
            Xs[i] != Xs[i - 1] && Xs2.push(Xs[i]) && xs.push(x + gutter + (Xs[i] - minx) * kx);
        }
        Xs = Xs2;
        ii = Xs.length;
        var cvrs = f || that.set();
        for (var i = 0; i < ii; i++) {
            var X = xs[i] - (xs[i] - (xs[i - 1] || x)) / 2,
                w = ((xs[i + 1] || x + width) - xs[i]) / 2 + (xs[i] - (xs[i - 1] || x)) / 2,
                C;
            f ? (C = {}) : cvrs.push(C = that.rect(X - 1, y, Math.max(w + 1, 1), height).attr({stroke: "none", fill: "#000", opacity: 0}));
            C.values = [];
            C.symbols = that.set();
            C.y = [];
            C.x = xs[i];
            C.axis = Xs[i];
            for (var j = 0, jj = valuesy.length; j < jj; j++) {
                Xs2 = valuesx[j] || valuesx[0];
                for (var k = 0, kk = Xs2.length; k < kk; k++) {
                    if (Xs2[k] == Xs[i]) {
                        C.values.push(valuesy[j][k]);
                        C.y.push(y + height - gutter - (valuesy[j][k] - miny) * ky);
                        C.symbols.push(chart.symbols[j][k]);
                    }
                }
            }
            f && f.call(C);
        }
        !f && (columns = cvrs);
    }
    function createDots(f) {
        var cvrs = f || that.set(),
            C;
        for (var i = 0, ii = valuesy.length; i < ii; i++) {
            for (var j = 0, jj = valuesy[i].length; j < jj; j++) {
                var X = x + gutter + ((valuesx[i] || valuesx[0])[j] - minx) * kx,
                    nearX = x + gutter + ((valuesx[i] || valuesx[0])[j ? j - 1 : 1] - minx) * kx,
                    Y = y + height - gutter - (valuesy[i][j] - miny) * ky;
                f ? (C = {}) : cvrs.push(C = that.circle(X, Y, Math.abs(nearX - X) / 2).attr({stroke: "none", fill: "#000", opacity: 0}));
                C.x = X;
                C.y = Y;
                C.value = valuesy[i][j];
                C.line = chart.lines[i];
                C.shade = chart.shades[i];
                C.symbol = chart.symbols[i][j];
                C.symbols = chart.symbols[i];
                C.axis = (valuesx[i] || valuesx[0])[j];
                f && f.call(C);
            }
        }
        !f && (dots = cvrs);
    }
    chart.push(lines, shades, symbols, axis, columns, dots);
    chart.lines = lines;
    chart.shades = shades;
    chart.symbols = symbols;
    chart.axis = axis;
    chart.chart_height = height;
    chart.y_pos = y;
    chart.x_pos = x;
    chart.colors = colors;
    chart.hoverColumn = function (fin, fout) {
        !columns && createColumns();
        columns.mouseover(fin).mouseout(fout);
        return this;
    };
    chart.clickColumn = function (f) {
        !columns && createColumns();
        columns.click(f);
        return this;
    };
    chart.hrefColumn = function (cols) {
        var hrefs = that.raphael.is(arguments[0], "array") ? arguments[0] : arguments;
        if (!(arguments.length - 1) && typeof cols == "object") {
            for (var x in cols) {
                for (var i = 0, ii = columns.length; i < ii; i++) if (columns[i].axis == x) {
                    columns[i].attr("href", cols[x]);
                }
            }
        }
        !columns && createColumns();
        for (var i = 0, ii = hrefs.length; i < ii; i++) {
            columns[i] && columns[i].attr("href", hrefs[i]);
        }
        return this;
    };
    chart.hover = function (fin, fout) {
        !dots && createDots();
        dots.mouseover(fin).mouseout(fout);
        return this;
    };
    chart.click = function (f) {
        !dots && createDots();
        dots.click(f);
        return this;
    };
    chart.each = function (f) {
        createDots(f);
        return this;
    };
    chart.eachColumn = function (f) {
        createColumns(f);
        return this;
    };
    return chart;
};

Raphael.fn.g.auto_linechart = function (raphael, x, y, width, height, valuesx, valuesy, opts, labelsx) {
	var chart = this.g.linechart(x, y, width, height, valuesx, valuesy, opts, labelsx).hoverColumn(function () {
	    this.tags = raphael.set();
	    this.legends = raphael.set();
	    this.symbols = raphael.set();
	    this.legend_y_pos = chart.y_pos + chart.chart_height + 20;
	    this.legend_x_pos = chart.x_pos;
	    this.colors = chart.colors;
	    for (var i = 0, ii = this.y.length; i < ii; i++) {
		if (this.values[i] != null)
		{
		   this.tags.push(raphael.g.tag(this.x, this.y[i], ""+this.values[i], 160, 10).insertBefore(this).attr([{fill: this.colors[i]}, {fill: "#fff"}]));
		   //this.symbols.push(raphael.g["disc"](this.x, this.y[i], 4).attr({fill: this.colors[i], stroke: "none"}));
           //(Raphael.is(sym, "array") ? sym[j] : sym) && symset.push(this.g["plus"](X, Y, (opts.width || 2) * 3).attr({fill: colors[i], stroke: "none"}));
		}
		raphael.rect(this.legend_x_pos, this.legend_y_pos+(i*16)-5, 10, 10).attr({stroke: "none", fill: this.colors[i]})
		this.legends.push(raphael.g.text(this.legend_x_pos+20, this.legend_y_pos+(i*16), "innodb_buffer_pool_hits_"+(this.values[i] != null ? this.values[i] : "N/A")).attr({"text-anchor": "start"}).attr(raphael.g.txtattr));
	    }
	}, function () {
	    this.tags && this.tags.remove();
	    this.legends && this.legends.remove();
	    this.symbols && this.symbols.remove();
	});
};

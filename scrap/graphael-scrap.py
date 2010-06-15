# Scrap code.
# You may ignore this file and its contents. IT is currently unused by the mycheckpoint project.

def create_report_graphael_chart_views(charts_list):
    ts_formats = {
        "sample": "%H:00",
        "hour":   "%D",
        "day":    "%b %e",
        }
    title_ts_formats = {
        "sample": "%b %e, %H:%i",
        "hour":   "%b %e, %H:00",
        "day":    "%b %e, %Y",
        }
    ts_present_map = {
        "sample": "IF(MINUTE(timeseries_ts) != 0, '', IF(HOUR(timeseries_ts) % 4 = 0, LOWER(DATE_FORMAT(timeseries_ts, '%H:00')), IF(HOUR(timeseries_ts) % 4 = 2, ' ', '|')))",
        "hour": "IF(MINUTE(timeseries_ts) != 0 OR HOUR(timeseries_ts) % 2 = 1, '', IF(HOUR(timeseries_ts) % 4 = 0, LOWER(DATE_FORMAT(timeseries_ts, '%H:00')), ' '))",
        "day": "IF(MINUTE(timeseries_ts) != 0 OR HOUR(timeseries_ts) % 2 = 1, '', IF(HOUR(timeseries_ts) % 4 = 0, LOWER(DATE_FORMAT(timeseries_ts, '%H:00')), ' '))",
        }
    for view_name_extension in ["sample", "hour", "day"]:
        charts_queries = []
        for (chart_columns, alias, _scale_from_0, _scale_to_100) in charts_list:
            chart_columns_list = [chart_column.strip() for chart_column in chart_columns.split(",")]
            chart_columns_query_clause = ", ',', ".join(["CONCAT('[', GROUP_CONCAT(IFNULL(ROUND(%s, 2), 'null') ORDER BY timeseries_ts SEPARATOR ','), ']')"  % chart_column for chart_column in chart_columns_list])
            chart_query = """CONCAT('[',
                GROUP_CONCAT(TIMESTAMPDIFF(MINUTE, ts_min, timeseries_ts) ORDER BY timeseries_ts SEPARATOR ','), 
                '],[',
                ${chart_columns_query_clause},
                '],{nostroke: false, axis: "0 0 1 1", symbol: null}, ', CONCAT('"${alias}\\\\n', DATE_FORMAT(ts_min, '${title_ts_format}'), '  -  ', DATE_FORMAT(ts_max, '${title_ts_format}')), '",[${column_names}], [',
                GROUP_CONCAT(CONCAT('"',
                    ${ts_present},
                    '"') ORDER BY timeseries_ts SEPARATOR ','), 
                ']') AS ${alias}""" 
            chart_query = chart_query.replace("${chart_columns_query_clause}", chart_columns_query_clause)
            chart_query = chart_query.replace("${alias}", alias)
            chart_query = chart_query.replace("${column_names}", ",".join(['"%s"' % chart_column for chart_column in chart_columns_list]))
            charts_queries.append(chart_query)
        charts_query = ",".join(charts_queries)
        query = """
            CREATE
            OR REPLACE
            ALGORITHM = TEMPTABLE
            DEFINER = CURRENT_USER
            SQL SECURITY INVOKER
            VIEW ${database_name}.sv_report_graphael_${view_name_extension} AS
              SELECT
                %s
              FROM
                ${database_name}.sv_report_chart_${view_name_extension}_timeseries,
                ${database_name}.sv_report_${view_name_extension}_recent_minmax
            """ % charts_query
        ts_present = ts_present_map[view_name_extension]
        title_ts_format = title_ts_formats[view_name_extension]
        custom_query = query
        custom_query = custom_query.replace("${title_ts_format}", title_ts_format)
        custom_query = custom_query.replace("${view_name_extension}", view_name_extension)
        custom_query = custom_query.replace("${ts_present}", ts_present)
        custom_query = custom_query.replace("${database_name}", database_name)
        act_query(custom_query)

    verbose("graphael views created")


def create_report_html_brief_interactive_graphael_view(report_charts):
    charts_sections_list = [chart_section for (chart_section, charts_aliases) in report_charts]
    chart_aliases_navigation_map = " | ".join(["""<a href="#%s">%s</a>""" % (chart_section, chart_section) for chart_section in charts_sections_list if chart_section])

    sections_queries = []
    js_queries = []
    for (chart_section, charts_aliases) in report_charts:
        charts_aliases_list = [chart_alias.strip() for chart_alias in charts_aliases.split(",")]
        charts_aliases_queries = []
        for chart_alias in charts_aliases_list:
            div_query = """'<div id="chartDiv_${chart_alias}" class="chart"></div>',
                """
            div_query = div_query.replace("${chart_alias}", chart_alias)
            charts_aliases_queries.append(div_query)

            js_query = """'
                    var r_${chart_alias} = Raphael("chartDiv_${chart_alias}");
                    r_${chart_alias}.g.auto_linechart(r_${chart_alias}, 50,20, ', chart_width-50, ', ',  chart_height-50, ', ', ${chart_alias},');'
                """ 
            js_query = js_query.replace("${chart_alias}", chart_alias)
            js_queries.append(js_query)
        charts_aliases_query = "".join(charts_aliases_queries)
        js_query = ",".join(js_queries)
        
        chart_section_anchor = chart_section 
        if not chart_section:
            chart_section_anchor = "section_%d" % len(sections_queries)
        section_query = """'
            <a name="%s"></a>
            <h2>%s <a href="#">[top]</a></h2>
            
            <div class="row">',
                %s
                '<div class="clear"></div>
            </div>
                ',
            """ % (chart_section_anchor, chart_section, charts_aliases_query)
        sections_queries.append(section_query)

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_html_brief_interactive AS
          SELECT CONCAT('
            <html>
                <head>
                    <title>mycheckpoint brief report</title>
                    <meta http-equiv="refresh" content="600" />
        <script src="/home/shlomi/workspace/mycheckpoint/graphael/raphael.js" type="text/javascript" charset="utf-8"></script>
        <script src="/home/shlomi/workspace/mycheckpoint/graphael/g.raphael.js" type="text/javascript" charset="utf-8"></script>
        <script src="/home/shlomi/workspace/mycheckpoint/graphael/g.line.js" type="text/javascript" charset="utf-8"></script>
        <script type="text/javascript" charset="utf-8">
            window.onload = function () {
        ', %s, '
            };
        </script>
                    <style type="text/css">
                    body {
                        color:#505050;
                        font-family: Verdana,Helvetica,Arial,sans-serif;
                        font-size:9pt;
                    }
                    div.row {
                        width: ${global_width};
                    }
                    div.chart {
                        float: left;
                        white-space: nowrap;
                        margin-right: 10px;
                        font-family: Helvetica,Verdana,Arial,sans-serif;
                        width: ', chart_width, 'px;
                    }
                    .clear {
                        clear:both;
                        height:1px;
                    }
                    div.chart {
                        float: left;
                    }
                    a {
                        color:#f26522;
                        text-decoration:none;
                    }
                    h2 {
                        font-weight:normal;
                        margin-top:20px;
                    }
                    h2 a {
                        font-weight:normal;
                        font-size: 60%%;
                    }
                    h4 {
                        font-family: Verdana,Helvetica,Arial,sans-serif;
                        display: inline;
                        margin-bottom: 0px;
                        padding-left: 8px;
                    }
                    .controls {
                        padding-left: 44px;
                        margin-bottom: 4px;
                    }
                    .controls button {
                        display: inline;
                    }
                    ul {
                        font-size:9pt;
                        margin: 0px;
                        padding-left: 44px;
                    }
                    li:first-child { 
                        list-style-type: none; 
                        font-weight: bold;
                    }
                    li { 
                        list-style-type: square; 
                    }
                    li div { 
                        color: #404040;
                        font-size:9pt;
                    }
                    </style>
                </head>
                <body>
                    <a name=""></a>
                    Report generated by <a href="http://code.openark.org/forge/mycheckpoint" target="mycheckpoint">mycheckpoint</a> on ',
                        DATE_FORMAT(NOW(),'%%b %%D %%Y, %%H:%%i'), '
                    <br/><br/>
                    Navigate: ${chart_aliases_navigation_map}
                    <br/>
                    ',
                    %s '
                </body>
            </html>
          ') AS html
          FROM
            ${database_name}.sv_report_graphael_sample, ${database_name}.charts_api
        """ % (js_query, "".join(sections_queries))
    query = query.replace("${database_name}", database_name)
    query = query.replace("${chart_aliases_navigation_map}", chart_aliases_navigation_map)
    query = query.replace("${global_width}", str(options.chart_width*3 + 30))

    act_query(query)

    verbose("sv_report_html_brief_interactive created")



def main():
    create_report_graphael_chart_views(report_chart_views)
    
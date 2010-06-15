# Scrap code.
# You may ignore this file and its contents. IT is currently unused by the mycheckpoint project.

def create_report_dygraph_chart_views(charts_list):
    for view_name_extension in ["sample", "hour", "day"]:
        charts_queries = []
        for (chart_columns, alias, _scale_from_0, _scale_to_100) in charts_list:
            non_breakable_chart_columns = chart_columns.replace(" ", "")
            chart_columns_list = chart_columns.split(",")
            chart_columns_query_clause = ", ',', ".join(["IFNULL(ROUND(%s, 2), '')" % chart_column for chart_column in chart_columns_list])
            charts_queries.append(
                "CONCAT('Date,%s\\\\n', GROUP_CONCAT(CONCAT(timeseries_ts, ',', %s, '\\\\n') ORDER BY timeseries_ts SEPARATOR '')) AS %s" % (non_breakable_chart_columns, chart_columns_query_clause, alias)
            )
            pass
        charts_query = ",".join(charts_queries)
        query = """
            CREATE
            OR REPLACE
            ALGORITHM = TEMPTABLE
            DEFINER = CURRENT_USER
            SQL SECURITY INVOKER
            VIEW ${database_name}.sv_report_dygraph_${view_name_extension} AS
              SELECT
                %s
              FROM
                ${database_name}.sv_report_chart_${view_name_extension}_timeseries
            """ % charts_query
        query = query.replace("${database_name}", database_name)
        query = query.replace("${view_name_extension}", view_name_extension)
        act_query(query)

    verbose("dycharts views created")



def create_report_html_brief_interactive_dygraph_view(report_charts):
    charts_sections_list = [chart_section for (chart_section, charts_aliases) in report_charts]
    chart_aliases_navigation_map = " | ".join(["""<a href="#%s">%s</a>""" % (chart_section, chart_section) for chart_section in charts_sections_list if chart_section])

    sections_queries = []
    for (chart_section, charts_aliases) in report_charts:
        charts_aliases_list = [chart_alias.strip() for chart_alias in charts_aliases.split(",")]
        charts_aliases_queries = []
        for chart_alias in charts_aliases_list:
            query = """
                '<div id = "chartDiv_${chart_alias}" class="chart">
                  <div class="controls"><button onClick="g_${chart_alias}.toggleMaximize()">+/-</button><h4>%s</h4></div>
                  <div id="graphDiv_${chart_alias}" class="graphdiv" style="width:', charts_api.chart_width, ';height:', charts_api.chart_height-40, '"></div>
                  <div id="labelsDiv_${chart_alias}" class="legend"></div>
                </div>
                <script type="text/javascript">
                    g_${chart_alias} = new Dygraph(
                        document.getElementById("graphDiv_${chart_alias}"),
                        "', IFNULL(${chart_alias}, ''), '",
                        {
                            chartDiv:  document.getElementById("chartDiv_${chart_alias}"),
                            labelsDiv: document.getElementById("labelsDiv_${chart_alias}")
                        }
                    );
                </script>',
                """ % (chart_alias.replace("_", " "),)
            query = query.replace("${chart_alias}", chart_alias)
            charts_aliases_queries.append(query)
        charts_aliases_query = "".join(charts_aliases_queries)
        
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
                    <script type="text/javascript" src="mycheckpoint/build/dygraphs/dygraph-combined.js"></script>
                    <title>mycheckpoint brief report</title>
                    <meta http-equiv="refresh" content="600" />
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
            ${database_name}.sv_report_dygraph_sample, ${database_name}.charts_api
        """ % "".join(sections_queries)
    query = query.replace("${database_name}", database_name)
    query = query.replace("${chart_aliases_navigation_map}", chart_aliases_navigation_map)
    query = query.replace("${global_width}", str(options.chart_width*3 + 30))

    act_query(query)

    verbose("sv_report_html_brief_interactive created")


def main():
    create_report_dygraph_chart_views(report_chart_views)

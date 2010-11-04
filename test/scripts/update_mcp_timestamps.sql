SELECT TIMESTAMPDIFF(WEEK, max(ts), NOW()) FROM mcp_sql00.status_variables INTO @diff;
UPDATE mcp_sql00.status_variables SET ts = ts + INTERVAL (@diff+1) WEEK ORDER BY ts DESC;

SELECT TIMESTAMPDIFF(WEEK, max(ts), NOW()) FROM mcp_sql01.status_variables INTO @diff;
UPDATE mcp_sql01.status_variables SET ts = ts + INTERVAL (@diff+1) WEEK ORDER BY ts DESC;


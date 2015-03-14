# mycheckpoint: lightweight, SQL oriented monitoring for MySQL #

## This project is discontinued. You're more than welcome to take over! ##

Visit **[mycheckpoint home page](http://code.openark.org/forge/mycheckpoint)**

_mycheckpoint_ is an open source monitoring utility for MySQL, with strong emphasis on user accessibility to monitored data.

It is SQL oriented: charts, reports and advances metrics are generated on the fly with views. There is no need for an external program to diagnose the data. In fact, _mycheckpoint’s_ main duty is to to a one-time creation of a special purpose schema.

Recording of data is performed via a single INSERT command, which takes measurements from GLOBAL VARIABLES, GLOBAL STATUS, MASTER STATUS and SLAVE STATUS. There is no requirement that this measurement is taken by _mycheckpoint_ itself. In addition, _mycheckpoint_ allows for [custom queries](http://code.openark.org/forge/mycheckpoint/documentation/custom-monitoring), the results of which are aggregated along with all other measurements.

Once the data is recorded, it is as easy to get complex presentations and diagnostics, such as [charting](http://code.openark.org/forge/mycheckpoint/documentation/generating-google-charts) and [reporting](http://code.openark.org/forge/mycheckpoint/documentation/generating-human-reports), as it is to issue [simple SELECT](http://code.openark.org/forge/mycheckpoint/documentation/querying-for-data) statements. You can even get [HTML reporting](http://code.openark.org/forge/mycheckpoint/documentation/generating-html-reports) pages (see [sample](http://code.openark.org/forge/wp-content/uploads/2010/07/mycheckpoint-report-brief-169.html)). [Detecting parameters change](http://code.openark.org/forge/mycheckpoint/documentation/detecting-parameters-change) is also easily achieved.

It is also possible to create [alert conditions](http://code.openark.org/forge/mycheckpoint/documentation/alerts), and get notified via [email](http://code.openark.org/forge/mycheckpoint/documentation/emails) on raised or resolved alerts.

Please refer to the [documentation](http://code.openark.org/forge/mycheckpoint/documentation) for further discussion, (or just use the [Quick HOWTO](http://code.openark.org/forge/mycheckpoint/documentation/quick-howto))


> ![http://code.openark.org/forge/wp-content/uploads/2010/07/mycheckpoint-report-html-screenshot.png](http://code.openark.org/forge/wp-content/uploads/2010/07/mycheckpoint-report-html-screenshot.png)

> _Above: HTML report partial screenshot_

> _Below: querying for human readable hourly report_


```
	Report period: 2010-02-10 08:00:00 to 2010-02-10 09:00:00. Period is 60 minutes (1.00 hours)
	Uptime: 100% (Up: 379 days, 02:12:28 hours)

	OS:
	    Load average: 2.70
	    CPU utilization: 17.8%
	    Memory: 7470.1MB used out of 8177.3MB (Active: 6920.9MB)
	    Swap: 4319.2MB used out of 8385.9MB

	InnoDB:
	    innodb_buffer_pool_size: 4718592000 bytes (4500.0MB). Used: 100.0%
	    Read hit: 99.92%
	    Disk I/O: 15.07 reads/sec  22.89 flushes/sec
	    Estimated log written per hour: 809.3MB
	    Locks: 0.13/sec  current: 0

	MyISAM key cache:
	    key_buffer_size: 33554432 bytes (32.0MB). Used: 18.2%
	    Read hit: 99.8%  Write hit: 100.0%

	DML:
	    SELECT:  82.97/sec  13.0%
	    INSERT:  66.70/sec  10.5%
	    UPDATE:  19.24/sec  3.0%
	    DELETE:  16.95/sec  2.7%
	    REPLACE: 0.00/sec  0.0%
	    SET:     158.79/sec  24.9%
	    COMMIT:  0.03/sec  0.0%
	    slow:    0.02/sec  0.0% (slow time: 1sec)

	Selects:
	    Full scan: 5.69/sec  6.9%
	    Full join: 0.00/sec  0.0%
	    Range:     0.84/sec  1.0%
	    Sort merge passes: 0.00/sec

	Locks:
	    Table locks waited:  0.00/sec  0.0%

	Tables:
	    Table cache: 2048. Used: 88.5%
	    Opened tables: 0.00/sec

	Temp tables:
	    Max tmp table size:  67108864 bytes (64.0MB)
	    Max heap table size: 67108864 bytes (64.0MB)
	    Created:             6.15/sec
	    Created disk tables: 0.18/sec  2.8%

	Connections:
	    Max connections: 200. Max used: 245  122.5%
	    Connections: 2.67/sec
	    Aborted:     0.07/sec  2.6%

	Threads:
	    Thread cache: 32. Used: 68.8%
	    Created: 0.02/sec

	Replication:
	    Master status file number: 2241, position: 524487864
	    Relay log space limit: 10737418240, used: N/A  (N/A%)
	    Seconds behind master: N/A
	    Estimated time for slave to catch up: N/A seconds (N/A days, N/A hours)  ETA: N/A
```
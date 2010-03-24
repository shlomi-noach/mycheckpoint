#!/usr/bin/python

#
# mycheckpoint: Lightweight, SQL oriented monitoring for MySQL
#
# Released under the BSD license
#
# Copyright (c) 2009-2010, Shlomi Noach
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
#     * Neither the name of the organization nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import ConfigParser
import getpass
import MySQLdb
import os
import sys
import socket
import traceback
import warnings
from optparse import OptionParser

import smtplib
try:
    MIMEText = __import__("email.mime.text", globals(), locals(), ["MIMEText"]).MIMEText
    MIMEMultipart = __import__("email.mime.multipart", globals(), locals(), ["MIMEMultipart"]).MIMEMultipart
except:
    try:
        MIMEText = __import__("email.MIMEText", globals(), locals(), ["MIMEText"]).MIMEText
        MIMEMultipart = __import__("email.MIMEMultipart", globals(), locals(), ["MIMEMultipart"]).MIMEMultipart
    except:
        pass



def parse_options():
    usage = """usage: mycheckpoint [options] [command [, command ...]]

Available commands:
  deploy
  email_brief_report
    """
    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL host. Written to by this application (default: localhost)")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default="3306", help="TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when host is localhost (default: /var/run/mysqld/mysql.sock)")
    parser.add_option("", "--monitored-host", dest="monitored_host", default=None, help="MySQL monitored host. Specity this when the host you're monitoring is not the same one you're writing to (default: none, host specified by --host is both monitored and written to)")
    parser.add_option("", "--monitored-port", dest="monitored_port", type="int", default="3306", help="Monitored host's TCP/IP port (default: 3306). Only applies when monitored-host is specified")
    parser.add_option("", "--monitored-socket", dest="monitored_socket", default="/var/run/mysqld/mysql.sock", help="Monitored host MySQL socket file. Only applies when monitored-host is specified and is localhost (default: /var/run/mysqld/mysql.sock)")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
    parser.add_option("-d", "--database", dest="database", default="mycheckpoint", help="Database name (required unless query uses fully qualified table names)")
    parser.add_option("", "--purge-days", dest="purge_days", type="int", default=182, help="Purge data older than specified amount of days (default: 182)")
    parser.add_option("", "--disable-bin-log", dest="disable_bin_log", action="store_true", default=False, help="Disable binary logging (binary logging enabled by default)")
    parser.add_option("", "--skip-disable-bin-log", dest="disable_bin_log", action="store_false", help="Skip disabling the binary logging (this is default behaviour; binary logging enabled by default)")
    parser.add_option("", "--skip-check-replication", dest="skip_check_replication", action="store_true", default=False, help="Skip checking on master/slave status variables")
    parser.add_option("-o", "--force-os-monitoring", dest="force_os_monitoring", action="store_true", default=False, help="Monitor OS even if monitored host does does nto appear to be the local host. Use when you are certain the monitored host is local")
    parser.add_option("", "--skip-alerts", dest="skip_alerts", action="store_true", default=False, help="Skip evaluating alert conditions as well as sending email notifications")
    parser.add_option("", "--skip-emails", dest="skip_emails", action="store_true", default=False, help="Skip sending email notifications")
    parser.add_option("", "--force-emails", dest="force_emails", action="store_true", default=False, help="Force sending email notifications even if there's nothing wrong")
    parser.add_option("", "--chart-width", dest="chart_width", type="int", default=400, help="Chart image width (default: 400, min value: 150)")
    parser.add_option("", "--chart-height", dest="chart_height", type="int", default=200, help="Chart image height (default: 200, min value: 100)")
    parser.add_option("", "--chart-service-url", dest="chart_service_url", default="http://chart.apis.google.com/chart", help="Url to Google charts API (default: http://chart.apis.google.com/chart)")
    parser.add_option("", "--smtp-host", dest="smtp_host", default=None, help="SMTP mail server host name or IP")
    parser.add_option("", "--smtp-from", dest="smtp_from", default=None, help="Address to use as mail sender")
    parser.add_option("", "--smtp-to", dest="smtp_to", default=None, help="Comma delimited email addresses to send emails to")
    parser.add_option("", "--debug", dest="debug", action="store_true", help="Print stack trace on error")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    return parser.parse_args()


def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    sys.stderr.write("-- ERROR: %s\n" % message)


def open_connections():
    if options.defaults_file:
        write_conn = MySQLdb.connect(
            read_default_file = options.defaults_file,
            db = database_name)
    else:
        if options.prompt_password:
            password=getpass.getpass()
        else:
            password=options.password
        write_conn = MySQLdb.connect(
            host = options.host,
            user = options.user,
            passwd = password,
            port = options.port,
            unix_socket = options.socket,
            db = database_name)

    # If no read (monitored) host specified, then read+write hosts are the same one...
    if not options.monitored_host:
        return write_conn, write_conn;

    # Need to open a read connection
    if options.defaults_file:
        monitored_conn = MySQLdb.connect(
            read_default_file = options.defaults_file,
            host = options.monitored_host,
            port = options.monitored_port,
            unix_socket = options.monitored_socket)
    else:
        monitored_conn = MySQLdb.connect(
            user = options.user,
            passwd = password,
            host = options.monitored_host,
            port = options.monitored_port,
            unix_socket = options.monitored_socket)

    return monitored_conn, write_conn;


def init_connections():
    query = """SET @@group_concat_max_len = GREATEST(@@group_concat_max_len, 65535)"""
    act_query(query, monitored_conn)
    act_query(query, write_conn)


def act_query(query, connection=None):
    """
    Run the given query, commit changes
    """
    if connection is None:
        connection = write_conn
    connection = write_conn
    cursor = connection.cursor()
    num_affected_rows = cursor.execute(query)
    cursor.close()
    connection.commit()
    return num_affected_rows


def get_row(query, connection=None):
    if connection is None:
        connection = monitored_conn
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    row = cursor.fetchone()

    cursor.close()
    return row


def get_rows(query, connection=None):
    if connection is None:
        connection = monitored_conn
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    return rows


def get_last_insert_id():
    query = "SELECT LAST_INSERT_ID() AS id"
    row = get_row(query, write_conn)
    return int(row["id"])


def prompt_deploy_instructions():
    print "--"
    print "-- Make sure `%s` schema exists, e.g." % database_name
    print "--   CREATE DATABASE `%s`" % database_name
    print "-- Make sure the user has ALL PRIVILEGES on the `%s` schema. e.g." % database_name
    print "--   GRANT ALL ON `%s`.* TO 'my_user'@'my_host' IDENTIFIED BY 'my_password'" % database_name
    print "-- The user will have to have the SUPER privilege in order to disable binary logging"
    print "-- - Otherwise, use --skip-disable-bin-log (but then be aware that slaves replicate this server's status)"
    print "-- In order to read master and slave status, the user must also be granted with REPLICATION CLIENT or SUPER privileges"
    print "-- - Otherwise, use --skip-check-replication"
    print "--"


def prompt_collect_instructions():
    print "--"
    print "-- Make sure you have executed mycheckpoint with 'deploy' after last install/update.upgrade"
    print "--  If not, run again with same configuration, and add 'deploy'. e.g.:"
    print "--  mycheckpoint --host=my_host deploy"
    print "--"


def get_monitored_host():
    monitored_host = options.monitored_host
    if not monitored_host:
        monitored_host = options.host
    return monitored_host


def is_local_monitoring():
    monitored_host = get_monitored_host()
    if monitored_host in ["localhost", "127.0.0.1"]:
        return True
    if monitored_host in [socket.getfqdn(), socket.gethostname()]:
        return True
    return False


def should_monitor_os():
    if options.force_os_monitoring:
        return True
    if is_local_monitoring():
        return True
    return False


def is_neglectable_variable(variable_name):
    if variable_name.startswith("ssl_"):
        return True
    if variable_name.startswith("ndb_"):
        return True
    if variable_name == "last_query_cost":
        return True
    return False


def normalize_variable_value(variable_value):
    if variable_value == "off":
        variable_value = 0
    elif variable_value == "on":
        variable_value = 1
    elif variable_value == "demand":
        variable_value = 2
    elif variable_value == "no":
        variable_value = 0
    elif variable_value == "yes":
        variable_value = 1
    return variable_value


def get_global_variables():
    global_variables = [
        "auto_increment_increment",
        "binlog_cache_size",
        "bulk_insert_buffer_size",
        "concurrent_insert",
        "connect_timeout",
        "delay_key_write",
        "delayed_insert_limit",
        "delayed_insert_timeout",
        "delayed_queue_size",
        "expire_logs_days",
        "foreign_key_checks",
        "group_concat_max_len",
        "innodb_additional_mem_pool_size",
        "innodb_autoextend_increment",
        "innodb_autoinc_lock_mode",
        "innodb_buffer_pool_size",
        "innodb_checksums",
        "innodb_commit_concurrency",
        "innodb_concurrency_tickets",
        "innodb_fast_shutdown",
        "innodb_file_io_threads",
        "innodb_file_per_table",
        "innodb_flush_log_at_trx_commit",
        "innodb_force_recovery",
        "innodb_lock_wait_timeout",
        "innodb_log_buffer_size",
        "innodb_log_file_size",
        "innodb_log_files_in_group",
        "innodb_max_dirty_pages_pct",
        "innodb_max_purge_lag",
        "innodb_mirrored_log_groups",
        "innodb_open_files",
        "innodb_rollback_on_timeout",
        "innodb_stats_on_metadata",
        "innodb_support_xa",
        "innodb_sync_spin_loops",
        "innodb_table_locks",
        "innodb_thread_concurrency",
        "innodb_thread_sleep_delay",
        "join_buffer_size",
        "key_buffer_size",
        "key_cache_age_threshold",
        "key_cache_block_size",
        "key_cache_division_limit",
        "large_files_support",
        "large_page_size",
        "large_pages",
        "locked_in_memory",
        "log_queries_not_using_indexes",
        "log_slow_queries",
        "long_query_time",
        "low_priority_updates",
        "max_allowed_packet",
        "max_binlog_cache_size",
        "max_binlog_size",
        "max_connect_errors",
        "max_connections",
        "max_delayed_threads",
        "max_error_count",
        "max_heap_table_size",
        "max_insert_delayed_threads",
        "max_join_size",
        "max_length_for_sort_data",
        "max_prepared_stmt_count",
        "max_relay_log_size",
        "max_seeks_for_key",
        "max_sort_length",
        "max_sp_recursion_depth",
        "max_tmp_tables",
        "max_user_connections",
        "max_write_lock_count",
        "min_examined_row_limit",
        "multi_range_count",
        "myisam_data_pointer_size",
        "myisam_max_sort_file_size",
        "myisam_repair_threads",
        "myisam_sort_buffer_size",
        "myisam_use_mmap",
        "net_buffer_length",
        "net_read_timeout",
        "net_retry_count",
        "net_write_timeout",
        "old_passwords",
        "open_files_limit",
        "optimizer_prune_level",
        "optimizer_search_depth",
        "port",
        "preload_buffer_size",
        "profiling",
        "profiling_history_size",
        "protocol_version",
        "pseudo_thread_id",
        "query_alloc_block_size",
        "query_cache_limit",
        "query_cache_min_res_unit",
        "query_cache_size",
        "query_cache_type",
        "query_cache_wlock_invalidate",
        "query_prealloc_size",
        "range_alloc_block_size",
        "read_buffer_size",
        "read_only",
        "read_rnd_buffer_size",
        "relay_log_space_limit",
        "rpl_recovery_rank",
        "server_id",
        "skip_external_locking",
        "skip_networking",
        "skip_show_database",
        "slave_compressed_protocol",
        "slave_net_timeout",
        "slave_transaction_retries",
        "slow_launch_time",
        "slow_query_log",
        "sort_buffer_size",
        "sql_auto_is_null",
        "sql_big_selects",
        "sql_big_tables",
        "sql_buffer_result",
        "sql_log_bin",
        "sql_log_off",
        "sql_log_update",
        "sql_low_priority_updates",
        "sql_max_join_size",
        "sql_notes",
        "sql_quote_show_create",
        "sql_safe_updates",
        "sql_select_limit",
        "sql_warnings",
        "sync_binlog",
        "sync_frm",
        "table_cache",
        "table_definition_cache",
        "table_lock_wait_timeout",
        "table_open_cache",
        "thread_cache_size",
        "thread_stack",
        "timed_mutexes",
        "timestamp",
        "tmp_table_size",
        "transaction_alloc_block_size",
        "transaction_prealloc_size",
        "unique_checks",
        "updatable_views_with_limit",
        "wait_timeout",
        "warning_count",
        ]
    return global_variables


def get_extra_variables():
    extra_variables = [
        "hostname",
        "datadir",
        "tmpdir",
        "version",
        ]
    return extra_variables


def get_mountpoint_usage_percent(path):
    """
    Find the mountpoint for the given path; return the integer number of disk used percent.
    """
    mountpoint = os.path.abspath(path)
    while not os.path.ismount(mountpoint):
        mountpoint = os.path.split(mountpoint)[0]

    statvfs = os.statvfs(mountpoint)
    #mount_usage = int(100-100.0*statvfs.f_bavail/statvfs.f_blocks)

    # The following calculation follows df.c (part of coreutils)
    # statvfs.f_blocks is total blocks
    # statvfs.f_bavail is available blocks
    # statvfs.f_bfree is blocks available to root

    used_blocks = statvfs.f_blocks - statvfs.f_bfree
    nonroot_total_blocks = used_blocks + statvfs.f_bavail

    used_percent = 100*used_blocks/nonroot_total_blocks
    if 100*used_blocks % nonroot_total_blocks != 0:
        used_percent = used_percent+1

    return used_percent


def get_additional_status_variables():
    additional_status_variables = [
        "queries",
        "open_table_definitions",
        "opened_table_definitions",
    ]
    custom_status_variables = ["custom_%d" % i for i in range(16)]
    additional_status_variables.extend(custom_status_variables)
    
    return additional_status_variables


def fetch_status_variables():
    """
    Fill in the status_dict. We make point of filling in all variables, even those not existing,
    for having the dictionary hold the keys. Based on these keys, tables and views are created.
    So it is important that we have the dictionary include all possible keys.
    """
    if status_dict:
        return status_dict

    # Make sure some status variables exist: these are required due to 5.0 - 5.1
    # or minor versions incompatibilities.
    for additional_status_variable in get_additional_status_variables():
        status_dict[additional_status_variable] = None
    query = "SHOW GLOBAL STATUS"
    rows = get_rows(query);
    for row in rows:
        variable_name = row["Variable_name"].lower()
        variable_value = row["Value"].lower()
        if not is_neglectable_variable(variable_name):
            status_dict[variable_name] = normalize_variable_value(variable_value)

    # Listing of interesting global variables:
    global_variables = get_global_variables()
    extra_variables = get_extra_variables()
    for variable_name in global_variables:
        status_dict[variable_name.lower()] = None
    query = "SHOW GLOBAL VARIABLES"
    rows = get_rows(query);
    for row in rows:
        variable_name = row["Variable_name"].lower()
        variable_value = row["Value"].lower()
        if variable_name in global_variables:
            status_dict[variable_name] = normalize_variable_value(variable_value)
        elif variable_name in extra_variables:
            extra_dict[variable_name] = variable_value

    verbose("Global status & variables recorded")

    # Master & slave status
    status_dict["master_status_position"] = None
    status_dict["master_status_file_number"] = None
    slave_status_variables = [
        "Read_Master_Log_Pos",
        "Relay_Log_Pos",
        "Exec_Master_Log_Pos",
        "Relay_Log_Space",
        "Seconds_Behind_Master",
        ]
    for variable_name in slave_status_variables:
        status_dict[variable_name.lower()] = None
    if not options.skip_check_replication:
        try:
            query = "SHOW MASTER STATUS"
            master_status = get_row(query)
            if master_status:
                status_dict["master_status_position"] = master_status["Position"]
                log_file_name = master_status["File"]
                log_file_number = int(log_file_name.rsplit(".")[-1])
                status_dict["master_status_file_number"] = log_file_number
            query = "SHOW SLAVE STATUS"
            slave_status = get_row(query)
            if slave_status:
                for variable_name in slave_status_variables:
                    status_dict[variable_name.lower()] = slave_status[variable_name]
            verbose("Master and slave status recorded")
        except:
            # An exception can be thrown if the user does not have enough privileges:
            print_error("Cannot show master & slave status. Skipping")
            pass

    # OS (linux) load average
    status_dict["os_loadavg_millis"] = None
    # OS (linux) CPU
    status_dict["os_cpu_user"] = None
    status_dict["os_cpu_nice"] = None
    status_dict["os_cpu_system"] = None
    status_dict["os_cpu_idle"] = None
    # OS Mem
    status_dict["os_mem_total_kb"] = None
    status_dict["os_mem_free_kb"] = None
    status_dict["os_mem_active_kb"] = None
    status_dict["os_swap_total_kb"] = None
    status_dict["os_swap_free_kb"] = None

    status_dict["os_root_mountpoint_usage_percent"] = None
    status_dict["os_datadir_mountpoint_usage_percent"] = None
    status_dict["os_tmpdir_mountpoint_usage_percent"] = None

    # We monitor OS params if this is the local machine, or --force-os-monitoring has been specified
    if should_monitor_os():
        try:
            f = open("/proc/stat")
            first_line = f.readline()
            f.close()

            tokens = first_line.split()
            os_cpu_user, os_cpu_nice, os_cpu_system, os_cpu_idle = tokens[1:5]
            status_dict["os_cpu_user"] = int(os_cpu_user)
            status_dict["os_cpu_nice"] = int(os_cpu_nice)
            status_dict["os_cpu_system"] = int(os_cpu_system)
            status_dict["os_cpu_idle"] = int(os_cpu_idle)
            verbose("OS CPU info recorded")
        except:
            verbose("Cannot read /proc/stat. Skipping")

        try:
            f = open("/proc/loadavg")
            first_line = f.readline()
            f.close()

            tokens = first_line.split()
            loadavg_1_min = float(tokens[0])
            loadavg_millis = int(loadavg_1_min * 1000)
            status_dict["os_loadavg_millis"] = loadavg_millis
            verbose("OS load average info recorded")
        except:
            verbose("Cannot read /proc/loadavg. Skipping")

        try:
            f = open("/proc/meminfo")
            lines = f.readlines()
            f.close()

            for line in lines:
                tokens = line.split()
                param_name = tokens[0].replace(":", "").lower()
                param_value = int(tokens[1])
                if param_name == "memtotal":
                    status_dict["os_mem_total_kb"] = param_value
                elif param_name == "memfree":
                    status_dict["os_mem_free_kb"] = param_value
                elif param_name == "active":
                    status_dict["os_mem_active_kb"] = param_value
                elif param_name == "swaptotal":
                    status_dict["os_swap_total_kb"] = param_value
                elif param_name == "swapfree":
                    status_dict["os_swap_free_kb"] = param_value
            verbose("OS mem info recorded")
        except:
            verbose("Cannot read /proc/meminfo. Skipping")

        # Filesystems:
        try:
            status_dict["os_root_mountpoint_usage_percent"] = get_mountpoint_usage_percent("/")
            status_dict["os_datadir_mountpoint_usage_percent"] = get_mountpoint_usage_percent(extra_dict["datadir"])
            status_dict["os_tmpdir_mountpoint_usage_percent"] = get_mountpoint_usage_percent(extra_dict["tmpdir"])
            verbose("OS mountpoints info recorded")
        except:
            verbose("Cannot read mountpoints info. Skipping")

    else:
        verbose("Non-local monitoring; will not read OS data")

    return status_dict


def get_status_variables_columns():
    """
    Return all columns participating in the status variables table. Most of these are STATUS variables.
    Others are parameters. Others yet represent slave or master status etc.
    """
    status_dict = fetch_status_variables()
    return sorted(status_dict.keys())


def get_variables_and_status_columns():
    variables_columns = get_global_variables()
    status_columns = [column_name for column_name in get_status_variables_columns() if not column_name in variables_columns]
    return variables_columns, status_columns


def is_signed_column(column_name):
    known_signed_diff_status_variables = [
        "threads_cached",
        "threads_connected",
        "threads_running",
        "open_table_definitions",
        "open_tables",
        "slave_open_temp_tables",
        "qcache_free_blocks",
        "qcache_free_memory",
        "qcache_queries_in_cache",
        "qcache_total_blocks",
        "innodb_page_size",
        "innodb_buffer_pool_pages_total",
        "innodb_buffer_pool_pages_free",
        "key_blocks_unused",
        "key_cache_block_size",
        "master_status_position",
        "read_master_log_pos",
        "relay_log_pos",
        "exec_master_log_pos",
        "relay_log_space",
        "seconds_behind_master",
        ]
    return column_name in known_signed_diff_status_variables


def get_column_sign_indicator(column_name):
    if is_signed_column(column_name):
        return "SIGNED"
    else:
        return "UNSIGNED"


def create_status_variables_table():
    columns_listing = ",\n".join(["%s BIGINT %s" % (column_name, get_column_sign_indicator(column_name)) for column_name in get_status_variables_columns()])
    query = """CREATE TABLE %s.%s (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            %s,
            UNIQUE KEY ts (ts)
       )
        """ % (database_name, table_name, columns_listing)

    table_created = False
    try:
        act_query(query)
        table_created = True
    except MySQLdb.Error:
        pass

    if table_created:
        verbose("%s table created" % table_name)
    else:
        verbose("%s table exists" % table_name)
    return table_created



def create_metadata_table():
    query = """
            DROP TABLE IF EXISTS %s.metadata
        """ % database_name
    try:
        act_query(query)
    except MySQLdb.Error:
        exit_with_error("Cannot execute %s" % query )

    query = """
        CREATE TABLE %s.metadata (
            revision SMALLINT UNSIGNED NOT NULL,
            build BIGINT UNSIGNED NOT NULL,
            last_deploy TIMESTAMP NOT NULL,
            mysql_version VARCHAR(255) CHARSET ascii NOT NULL
        )
        """ % database_name

    try:
        act_query(query)
        verbose("metadata table created")
    except MySQLdb.Error:
        exit_with_error("Cannot create table %s.metadata" % database_name)

    query = """
        REPLACE INTO %s.metadata
            (revision, build, mysql_version)
        VALUES
            (%d, %d, '%s')
        """ % (database_name, revision_number, build_number, get_monitored_host_mysql_version())
    act_query(query)


def create_numbers_table():
    query = """
            DROP TABLE IF EXISTS %s.numbers
        """ % database_name
    try:
        act_query(query)
    except MySQLdb.Error:
        exit_with_error("Cannot execute %s" % query )

    query = """
        CREATE TABLE %s.numbers (
            n SMALLINT UNSIGNED NOT NULL,
            PRIMARY KEY (n)
        )
        """ % database_name

    try:
        act_query(query)
        verbose("numbers table created")
    except MySQLdb.Error:
        exit_with_error("Cannot create table %s.numbers" % database_name)

    numbers_values = ",".join(["(%d)" % n for n in range(0,4096)])
    query = """
        INSERT IGNORE INTO %s.numbers
        VALUES %s
        """ % (database_name, numbers_values)
    act_query(query)


def create_charts_api_table():
    query = """
            DROP TABLE IF EXISTS %s.charts_api
        """ % database_name
    try:
        act_query(query)
    except MySQLdb.Error:
        exit_with_error("Cannot execute %s" % query )

    query = """
        CREATE TABLE %s.charts_api (
            chart_width SMALLINT UNSIGNED NOT NULL,
            chart_height SMALLINT UNSIGNED NOT NULL,
            simple_encoding CHAR(62) CHARSET ascii COLLATE ascii_bin,
            service_url VARCHAR(128) CHARSET ascii COLLATE ascii_bin
        )
        """ % database_name

    try:
        act_query(query)
        verbose("charts_api table created")
    except MySQLdb.Error:
        exit_with_error("Cannot create table %s.charts_api" % database_name)

    query = """
        INSERT INTO %s.charts_api
            (chart_width, chart_height, simple_encoding, service_url)
        VALUES
            (%d, %d, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', '%s')
        """ % (database_name, options.chart_width, options.chart_height, options.chart_service_url.replace("'", "''"))
    act_query(query)


def create_alert_condition_table():
    query = """
        CREATE TABLE IF NOT EXISTS %s.alert_condition (
          alert_condition_id INT UNSIGNED AUTO_INCREMENT,
          enabled BOOL NOT NULL DEFAULT 1,
          condition_eval VARCHAR(4095) CHARSET utf8 COLLATE utf8_bin NOT NULL,
          description VARCHAR(255) CHARSET utf8 COLLATE utf8_bin DEFAULT NULL,
          error_level ENUM('debug', 'info', 'warning', 'error', 'critical') NOT NULL DEFAULT 'error',
          alert_delay_minutes SMALLINT UNSIGNED NOT NULL DEFAULT 0,
          repetitive_alert BOOL NOT NULL DEFAULT 0,
          PRIMARY KEY (alert_condition_id)
        )
        """ % database_name

    try:
        act_query(query)
        verbose("alert_condition table created")
    except MySQLdb.Error:
        if options.debug:
            traceback.print_exc()
        exit_with_error("Cannot create table %s.alert_condition" % database_name)


def create_alert_table():
    query = """
        CREATE TABLE IF NOT EXISTS %s.alert (
          `alert_id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
          `alert_condition_id` INT(11) UNSIGNED NOT NULL,
          `sv_report_sample_id` INT(11) DEFAULT NULL,
          PRIMARY KEY (`alert_id`),
          UNIQUE KEY `alert_condition_sv_report_sample` (`sv_report_sample_id`, `alert_condition_id`),
          KEY `alert_condition_id` (`alert_condition_id`)
        )
        """ % database_name

    try:
        act_query(query)
        verbose("alert table created")
    except MySQLdb.Error:
        if options.debug:
            traceback.print_exc()
        exit_with_error("Cannot create table %s.alert" % database_name)


def create_alert_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.alert_view AS
          SELECT
            alert_condition.alert_condition_id,
            sv_report_sample.id AS sv_report_sample_id,
            TRIM(alert_condition.condition_eval) AS condition_eval,
            TRIM(alert_condition.description) AS description,
            alert_condition.error_level AS error_level,
            sv_report_sample.ts AS ts
          FROM
            ${database_name}.alert
            JOIN ${database_name}.alert_condition ON (alert_condition.alert_condition_id = alert.alert_condition_id)
            JOIN ${database_name}.sv_report_sample ON (alert.sv_report_sample_id = sv_report_sample.id)
          ORDER BY 
            alert.sv_report_sample_id, alert.alert_condition_id
    """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("alert_view created")


def create_alert_pending_table():
    query = """
        CREATE TABLE IF NOT EXISTS %s.alert_pending (
          alert_pending_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
          alert_condition_id INT(11) UNSIGNED NOT NULL,
          sv_report_sample_id_start INT(11) DEFAULT NULL,
          sv_report_sample_id_end INT(11) DEFAULT NULL,
          ts_notified DATETIME DEFAULT NULL,
          resolved BOOL NOT NULL DEFAULT 0,
          PRIMARY KEY (`alert_pending_id`),
          UNIQUE KEY (`alert_condition_id`)
        )
        """ % database_name

    try:
        act_query(query)
        verbose("alert_pending table created")
    except MySQLdb.Error:
        if options.debug:
            traceback.print_exc()
        exit_with_error("Cannot create table %s.alert_pending" % database_name)


def create_alert_pending_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.alert_pending_view AS
          SELECT
            alert_pending.alert_pending_id AS alert_pending_id,
            alert_condition.alert_condition_id AS alert_condition_id,
            TRIM(alert_condition.condition_eval) AS condition_eval,
            TRIM(alert_condition.description) AS description,
            alert_condition.error_level AS error_level,
            alert_condition.alert_delay_minutes AS alert_delay_minutes,
            sv_report_sample_start.ts AS ts_start,
            sv_report_sample_end.ts AS ts_end,
            (TIMESTAMPDIFF(SECOND, sv_report_sample_start.ts, sv_report_sample_end.ts)+3) DIV 60 AS elapsed_minutes, 
            (TIMESTAMPDIFF(SECOND, sv_report_sample_start.ts, sv_report_sample_end.ts)+3) DIV 60 >= alert_delay_minutes AS in_error,
            alert_pending.ts_notified IS NOT NULL AS is_notified,
            alert_pending.ts_notified AS ts_notified,
            alert_pending.resolved AS resolved,
            alert_condition.repetitive_alert AS repetitive_alert 
          FROM
            ${database_name}.alert_pending
            JOIN ${database_name}.alert_condition ON (alert_pending.alert_condition_id = alert_condition.alert_condition_id)
            JOIN ${database_name}.sv_report_sample AS sv_report_sample_start ON (alert_pending.sv_report_sample_id_start = sv_report_sample_start.id)
            JOIN ${database_name}.sv_report_sample AS sv_report_sample_end ON (alert_pending.sv_report_sample_id_end = sv_report_sample_end.id)
          ORDER BY
            resolved ASC,
            in_error DESC,
            error_level DESC,
            elapsed_minutes DESC,
            alert_condition_id ASC
    """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("alert_pending_view created")


def create_alert_pending_html_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.alert_pending_html_view AS
          SELECT
            CONCAT('
    <html>
        <head>
        <title>mycheckpoint pending alerts</title>
        <meta http-equiv="refresh" content="600" />
        <style type="text/css">
            body {
                background:#FFFFFF none repeat scroll 0% 0%;
                color:#505050;
                font-family:Verdana,Arial,Helvetica,sans-serif;
                font-size:9pt;
                line-height:1.5;
            }
            a {
                color:#f26522;
                text-decoration:none;
            }
            .nobr {
                white-space: nowrap;
            }
            div.table {
                display: table;
                border: 1px solid #d0d0d0;
            }
            div.row {
                display: table-row;
            }
            div.header {
                font-weight: bold;
            }
            .italic {
                font-style: italic;
            }
            div.row div {
                display: table-cell;
                border: 1px solid #d0d0d0;
                margin: 3px;
                padding: 3px;
            }
            .el_debug {
                color: #000000;
                background-color: #ffffff;
            }
            .el_info {
                color: #ffffff;
                background-color: #0000ff;
            }
            .el_warning {
                color: #000000;
                background-color: #ffff00;
            }
            .el_error {
                color: #ffffff;
                background-color: #ff0000;
            }
            .el_critical {
                color: #ffffff;
                background-color: #000000;
            }
        </style>
        </head>
        <body>
            Pending alerts report generated by <a href="http://code.openark.org/forge/mycheckpoint" target="mycheckpoint">mycheckpoint</a> on ',
                DATE_FORMAT(NOW(),'%b %D %Y, %H:%i'), '
            <br/><br/>
            <div class="table">
                <div class="row header">
                  <div>Error level</div> 
                  <div>Description</div> 
                  <div>Alert start time</div> 
                  <div>Elapsed minutes</div> 
                  <div>Notification time</div>
                  <div>Repeating notification</div>
                </div> 
                ',
                IFNULL(
                  GROUP_CONCAT(
                    CONCAT(
                      '<div class="row">',
                        '<div class="el_', error_level, '">', error_level, '</div>', 
                        '<div>', description, '</div>', 
                        '<div>', ts_start, '</div>', 
                        '<div>', elapsed_minutes, '</div>', 
                        '<div', IF(ts_notified IS NULL, ' class="italic"', ''), '>', IFNULL(ts_notified, CONCAT('ETA: ', ts_start+ INTERVAL alert_delay_minutes MINUTE)), '</div>', 
                        '<div>', IF(repetitive_alert, 'Yes', '-'), '</div>', 
                      '</div>')
                    SEPARATOR ''), 
                  '')
                ,'
            </div>
            ',
            IF(GROUP_CONCAT(alert_pending_id) IS NULL, 
              '<br/>
              <div>
                There are no pending alerts
              </div>', 
              '') ,'
        </body>
    </html>
            ') AS html
          FROM
            alert_pending_view
          WHERE
            resolved = 0
    """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("alert_pending_html_view created")


def create_alert_email_message_items_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.alert_email_message_items_view AS
          SELECT
            alert_pending_id,
            IF(
              resolved,
              CONCAT(
                'Resolved: ', description, ' (pending id: ', alert_pending_id, ')'),
              CONCAT(
                UPPER(error_level), ': ', description, ' (pending id: ', alert_pending_id, ')
    This ', error_level, ' alert is pending for ', elapsed_minutes, ' minutes, since ', ts_start              
              )
            ) AS message_item
          FROM
            ${database_name}.alert_pending_view
          WHERE
            in_error > 0
            AND ((is_notified = 0) OR (repetitive_alert != 0) OR (resolved = 1))
    """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("alert_email_message_items_view created")


def create_alert_condition_query_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.alert_condition_query_view AS
          SELECT
            CONCAT(
              'SELECT ',
              GROUP_CONCAT(
                CONCAT(condition_eval, ' AS condition_', alert_condition_id) 
                SEPARATOR ' ,'),
              ' FROM ${database_name}.sv_report_sample
              ORDER BY id DESC 
              LIMIT 1;'
            ) AS query
          FROM
            ${database_name}.alert_condition
          WHERE
            enabled = 1
        """
    query = query.replace("${database_name}", database_name)

    try:
        act_query(query)
        verbose("alert_condition query view created")
    except MySQLdb.Error:
        exit_with_error("Cannot create view %s.alert_condition_query_view" % database_name)


def generate_alert_condition_query():
    query = """
            SELECT 
              alert_condition_id, condition_eval 
            FROM 
              ${database_name}.alert_condition
            WHERE
              enabled = 1
        """
    query = query.replace("${database_name}", database_name)
    rows = get_rows(query)
    if not rows:
        return (None, None)
    
    alert_condition_ids = [int(row["alert_condition_id"]) for row in rows]

    query_conditions = ["%s AS condition_%d" % (row["condition_eval"], int(row["alert_condition_id"])) for row in rows]
    query = """
        SELECT
          id, 
          %s 
        FROM
          ${database_name}.sv_report_sample
        ORDER BY 
          id DESC
        LIMIT 1
      """ % ",".join(query_conditions)
    query = query.replace("${database_name}", database_name)
    return alert_condition_ids, query


def write_alert(alert_condition_id, report_sample_id):
    query = """
        INSERT /*! IGNORE */ INTO 
          ${database_name}.alert (alert_condition_id, sv_report_sample_id) 
        VALUES 
          (%d, %d)
        """ % (alert_condition_id, report_sample_id)
    query = query.replace("${database_name}", database_name)
    act_query(query)



def write_alert_pending(alert_condition_id, report_sample_id):
    query = """
        INSERT INTO 
          ${database_name}.alert_pending (alert_condition_id, sv_report_sample_id_start, sv_report_sample_id_end) 
        VALUES 
          (%d, %d, %d)
        ON DUPLICATE KEY UPDATE
          sv_report_sample_id_end = %d
        """ % (alert_condition_id, report_sample_id, report_sample_id, report_sample_id)
    query = query.replace("${database_name}", database_name)
    act_query(query)
    
    
def mark_resolved_alerts(report_sample_id):
    query = """
        UPDATE 
            ${database_name}.alert_pending
        SET
          resolved = 1
        WHERE
          sv_report_sample_id_end < %d
        """ % report_sample_id
    query = query.replace("${database_name}", database_name)
    num_affected_rows = act_query(query)
    verbose("Marked %d pending alerts as resolved" % num_affected_rows)
    
    
def remove_resolved_alerts():
    query = """
        DELETE FROM 
            ${database_name}.alert_pending
        WHERE
          resolved = 1
        """
    query = query.replace("${database_name}", database_name)
    num_affected_rows = act_query(query)
    verbose("Deleted %d resolved pending alerts" % num_affected_rows)


def mark_notified_pending_alerts(notified_pending_alert_ids):    
    if not notified_pending_alert_ids:
        return

    query = """
        UPDATE 
          ${database_name}.alert_pending
        SET 
          ts_notified = NOW()
        WHERE 
          alert_pending_id IN (%s)
        """ % ",".join(["%d" % notified_pending_alert_id for notified_pending_alert_id in notified_pending_alert_ids])
    query = query.replace("${database_name}", database_name)
    act_query(query)


def check_alerts():
    if options.skip_alerts:
        verbose("Skipping alerts")
        return

    alert_condition_ids, query = generate_alert_condition_query()
    if not alert_condition_ids:
        verbose("No alert conditions defined")
        return
    
    row = get_row(query, write_conn)
    report_sample_id = int(row["id"])
    num_alerts = 0
    
    for alert_condition_id in alert_condition_ids:
        condition_result = row["condition_%d" % alert_condition_id]
        if condition_result is not None:
            if int(condition_result) != 0:
                write_alert(alert_condition_id, report_sample_id)
                write_alert_pending(alert_condition_id, report_sample_id)
                num_alerts += 1
    verbose("Found %s alerts" % num_alerts)
    mark_resolved_alerts(report_sample_id)
    
    notified_pending_alert_ids = send_alert_email()
    if notified_pending_alert_ids:
        # Alerts which have been notified must be marked as such
        mark_notified_pending_alerts(notified_pending_alert_ids)
    remove_resolved_alerts()


def send_alert_email():
    """
    Send an email including all never-sent pending alerts.
    Returns the ids of pending alerts
    """    
    query = """SELECT message_item, alert_pending_id FROM ${database_name}.alert_email_message_items_view"""
    query = query.replace("${database_name}", database_name)
    
    rows = get_rows(query, write_conn)
    if options.skip_emails and rows:
        verbose("--skip-emails requested. Not sending alert mail, although there are %d unnotified alerts" % len(rows))
        return None

    if not rows:
        # No problems / resolved problems to report
        if not options.force_emails:
            return None
        # Force an OK email    
        email_message = """
Database OK: %s

This is an alert mail sent by mycheckpoint, monitoring your %s MySQL database.
All seems to be well.
                """ % (database_name, database_name,)
        email_subject = "%s: mycheckpoint OK notification" % database_name
        send_email_message("alert notifications", email_subject, email_message)
        return None
        

    message_items = [row["message_item"] for row in rows]
    alert_pending_ids = [row["alert_pending_id"] for row in rows]
    
    resolved_item_found = False
    email_rows = []
    for message_item in message_items:
        if message_item.find("Resolved:") == 0:
            if not resolved_item_found:
                resolved_item_found = True
                email_rows.append("---")
        email_rows.append(message_item)
        
    email_message = """
Database alert: %s

This is an alert mail sent by mycheckpoint, monitoring your %s MySQL database.
The following problems have been found:

%s
        """ % (database_name, database_name, "\n".join(email_rows))
    email_subject = "%s: mycheckpoint alert notification" % database_name
    if send_email_message("alert notifications", email_subject, email_message):
        return alert_pending_ids
    else:
        return None


def get_monitored_host_mysql_version():
    version = get_row("SELECT VERSION() AS version")["version"]
    return version

def is_same_deploy():
    try:
        query = "SELECT COUNT(*) AS same_deploy FROM %s.metadata WHERE revision = %d AND build = %d AND mysql_version = '%s'" % (database_name, revision_number, build_number, get_monitored_host_mysql_version())
        same_deploy = get_row(query, write_conn)["same_deploy"]
        return (same_deploy > 0)
    except:
        return False


def upgrade_status_variables_table():

    # I currently prefer SHOW COLUMNS over using INFORMATION_SCHEMA because of the time it takes
    # to access the INFORMATION_SCHEMA.COLUMNS table.
    query = """
            SHOW COLUMNS FROM %s.%s
        """ % (database_name, table_name)
    existing_columns = [row["Field"] for row in get_rows(query, write_conn)]

    new_columns = [column_name for column_name in get_status_variables_columns() if column_name not in existing_columns]

    if new_columns:
        verbose("Will add the following columns to %s: %s" % (table_name, ", ".join(new_columns)))
        columns_listing = ",\n".join(["ADD COLUMN %s BIGINT %s" % (column_name, get_column_sign_indicator(column_name)) for column_name in new_columns])
        query = """ALTER TABLE %s.%s
                %s
        """ % (database_name, table_name, columns_listing)
        act_query(query)
        verbose("status_variables table upgraded")


def create_status_variables_latest_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_latest AS
          SELECT
            MAX(id) AS id_latest,
            MAX(ts) AS ts_latest
          FROM
            ${database_name}.${status_variables_table_name}
    """
    query = query.replace("${database_name}", database_name)
    query = query.replace("${status_variables_table_name}", table_name)
    act_query(query)

    verbose("sv_latest view created")


def create_status_variables_diff_view():
    global_variables, status_columns = get_variables_and_status_columns()
    # Global variables are used as-is
    global_variables_columns_listing = ",\n".join([" ${status_variables_table_alias}2.%s AS %s" % (column_name, column_name,) for column_name in global_variables])
    # status variables as they were:
    status_columns_listing = ",\n".join([" ${status_variables_table_alias}2.%s AS %s" % (column_name, column_name,) for column_name in status_columns])
    # Status variables are diffed. This does not make sense for all of them, but we do it for all nonetheless.
    diff_signed_columns_listing = ",\n".join([" ${status_variables_table_alias}2.%s - ${status_variables_table_alias}1.%s AS %s_diff" % (column_name, column_name, column_name, ) for column_name in status_columns if is_signed_column(column_name)])
    diff_unsigned_columns_listing = ",\n".join([" IF(${status_variables_table_alias}2.%s >= ${status_variables_table_alias}1.%s , ${status_variables_table_alias}2.%s - ${status_variables_table_alias}1.%s, ${status_variables_table_alias}2.%s) AS %s_diff" % (column_name, column_name, column_name, column_name, column_name, column_name, ) for column_name in status_columns if not is_signed_column(column_name)])

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = MERGE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_diff AS
          SELECT
            ${status_variables_table_name}2.id,
            ${status_variables_table_name}2.ts,
            TIMESTAMPDIFF(SECOND, ${status_variables_table_name}1.ts, ${status_variables_table_name}2.ts) AS ts_diff_seconds,
            %s,
            %s,
            %s,
            %s
          FROM
            ${database_name}.${status_variables_table_name} AS ${status_variables_table_alias}2
            INNER JOIN ${database_name}.${status_variables_table_name} AS ${status_variables_table_alias}1
            ON (${status_variables_table_alias}1.id = ${status_variables_table_alias}2.id-GREATEST(1, IFNULL(${status_variables_table_alias}2.auto_increment_increment, 1)))
    """ % (status_columns_listing, diff_signed_columns_listing, diff_unsigned_columns_listing, global_variables_columns_listing)
    query = query.replace("${database_name}", database_name)
    query = query.replace("${status_variables_table_name}", table_name)
    query = query.replace("${status_variables_table_alias}", table_name)
    act_query(query)

    verbose("sv_diff view created")


def create_status_variables_sample_view():
    global_variables, status_columns = get_variables_and_status_columns()

    global_variables_columns_listing = ",\n".join(["%s" % (column_name,) for column_name in global_variables])
    status_columns_listing = ",\n".join([" %s" % (column_name,) for column_name in status_columns])
    diff_columns_listing = ",\n".join([" %s_diff" % (column_name,) for column_name in status_columns])
    change_psec_columns_listing = ",\n".join([" ROUND(%s_diff/ts_diff_seconds, 2) AS %s_psec" % (column_name, column_name,) for column_name in status_columns])

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = MERGE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_sample AS
          SELECT
            id,
            ts,
            ts_diff_seconds,
            %s,
            %s,
            %s,
            %s
          FROM
            ${database_name}.sv_diff
        """ % (status_columns_listing, diff_columns_listing, change_psec_columns_listing, global_variables_columns_listing)
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_sample view created")


def create_status_variables_hour_view():
    global_variables, status_columns = get_variables_and_status_columns()

    global_variables_columns_listing = ",\n".join([" MAX(%s) AS %s" % (column_name, column_name,) for column_name in global_variables])
    status_columns_listing = ",\n".join([" MAX(%s) AS %s" % (column_name, column_name,) for column_name in status_columns])
    sum_diff_columns_listing = ",\n".join([" SUM(%s_diff) AS %s_diff" % (column_name, column_name,) for column_name in status_columns])
    avg_psec_columns_listing = ",\n".join([" ROUND(AVG(%s_psec), 2) AS %s_psec" % (column_name, column_name,) for column_name in status_columns])
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_hour AS
          SELECT
            MIN(id) AS id,
            DATE(ts) + INTERVAL HOUR(ts) HOUR AS ts,
            DATE(ts) + INTERVAL (HOUR(ts) + 1) HOUR AS end_ts,
            SUM(ts_diff_seconds) AS ts_diff_seconds,
            %s,
            %s,
            %s,
            %s
          FROM
            ${database_name}.sv_sample
          GROUP BY DATE(ts), HOUR(ts)
    """ % (status_columns_listing, sum_diff_columns_listing, avg_psec_columns_listing, global_variables_columns_listing)
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_hour view created")


def create_status_variables_day_view():
    global_variables, status_columns = get_variables_and_status_columns()

    global_variables_columns_listing = ",\n".join([" MAX(%s) AS %s" % (column_name, column_name,) for column_name in global_variables])
    status_columns_listing = ",\n".join([" MAX(%s) AS %s" % (column_name, column_name,) for column_name in status_columns])
    sum_diff_columns_listing = ",\n".join([" SUM(%s_diff) AS %s_diff" % (column_name, column_name,) for column_name in status_columns])
    avg_psec_columns_listing = ",\n".join([" ROUND(AVG(%s_psec), 2) AS %s_psec" % (column_name, column_name,) for column_name in status_columns])
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_day AS
          SELECT
            MIN(id) AS id,
            DATE(ts) AS ts,
            DATE(ts) + INTERVAL 1 DAY AS end_ts,
            SUM(ts_diff_seconds) AS ts_diff_seconds,
            %s,
            %s,
            %s,
            %s
          FROM
            ${database_name}.sv_sample
          GROUP BY DATE(ts)
    """ % (status_columns_listing, sum_diff_columns_listing, avg_psec_columns_listing, global_variables_columns_listing)
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_day view created")



def create_report_human_views():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = MERGE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_human_${view_name_extension} AS
          SELECT
            id,
            ts,
            CONCAT(
'Report period: ', TIMESTAMP(ts), ' to ', TIMESTAMP(ts) + INTERVAL ROUND(ts_diff_seconds/60) MINUTE, '. Period is ', ROUND(ts_diff_seconds/60), ' minutes (', round(ts_diff_seconds/60/60, 2), ' hours)
Uptime: ', uptime_percent,
    '% (Up: ', FLOOR(uptime/(60*60*24)), ' days, ', SEC_TO_TIME(uptime % (60*60*24)), ' hours)

OS:
    Load average: ', IFNULL(os_loadavg, 'N/A'), '
    CPU utilization: ', IFNULL(os_cpu_utilization_percent, 'N/A'), '%
    Memory: ', IFNULL(os_mem_used_mb, 'N/A'), 'MB used out of ', IFNULL(os_mem_total_mb, 'N/A'), 'MB (Active: ', IFNULL(os_mem_active_mb, 'N/A'), 'MB)
    Swap: ', IFNULL(os_swap_used_mb, 'N/A'), 'MB used out of ', IFNULL(os_swap_total_mb, 'N/A'), 'MB
    Mountpoints usage: root ', os_root_mountpoint_usage_percent, '%, datadir ', os_datadir_mountpoint_usage_percent, '%, tmpdir ', os_tmpdir_mountpoint_usage_percent, '%

InnoDB:
    innodb_buffer_pool_size: ', innodb_buffer_pool_size, ' bytes (', ROUND(innodb_buffer_pool_size/(1024*1024), 1), 'MB). Used: ',
        IFNULL(innodb_buffer_pool_used_percent, 'N/A'), '%
    Read hit: ', IFNULL(innodb_read_hit_percent, 'N/A'), '%
    Disk I/O: ', innodb_buffer_pool_reads_psec, ' reads/sec  ', innodb_buffer_pool_pages_flushed_psec, ' flushes/sec
    Estimated log written per hour: ', IFNULL(innodb_estimated_log_mb_written_per_hour, 'N/A'), 'MB
    Locks: ', innodb_row_lock_waits_psec, '/sec  current: ', innodb_row_lock_current_waits, '

MyISAM key cache:
    key_buffer_size: ', key_buffer_size, ' bytes (', ROUND(key_buffer_size/1024/1024, 1), 'MB). Used: ', IFNULL(key_buffer_used_percent, 'N/A'), '%
    Read hit: ', IFNULL(key_read_hit_percent, 'N/A'), '%  Write hit: ', IFNULL(key_write_hit_percent, 'N/A'), '%

DML:
    SELECT:  ', com_select_psec, '/sec  ', IFNULL(com_select_percent, 'N/A'), '%
    INSERT:  ', com_insert_psec, '/sec  ', IFNULL(com_insert_percent, 'N/A'), '%
    UPDATE:  ', com_update_psec, '/sec  ', IFNULL(com_update_percent, 'N/A'), '%
    DELETE:  ', com_delete_psec, '/sec  ', IFNULL(com_delete_percent, 'N/A'), '%
    REPLACE: ', com_replace_psec, '/sec  ', IFNULL(com_replace_percent, 'N/A'), '%
    SET:     ', com_set_option_psec, '/sec  ', IFNULL(com_set_option_percent, 'N/A'), '%
    COMMIT:  ', com_commit_psec, '/sec  ', IFNULL(com_commit_percent, 'N/A'), '%
    slow:    ', slow_queries_psec, '/sec  ', IFNULL(slow_queries_percent, 'N/A'), '% (slow time: ',
        long_query_time ,'sec)

Selects:
    Full scan: ', select_scan_psec, '/sec  ', IFNULL(select_scan_percent, 'N/A'), '%
    Full join: ', select_full_join_psec, '/sec  ', IFNULL(select_full_join_percent, 'N/A'), '%
    Range:     ', select_range_psec, '/sec  ', IFNULL(select_range_percent, 'N/A'), '%
    Sort merge passes: ', sort_merge_passes_psec, '/sec

Locks:
    Table locks waited:  ', table_locks_waited_psec, '/sec  ', IFNULL(table_lock_waited_percent, 'N/A'), '%

Tables:
    Table cache: ', table_cache_size, '. Used: ',
        IFNULL(table_cache_use_percent, 'N/A'), '%
    Opened tables: ', opened_tables_psec, '/sec

Temp tables:
    Max tmp table size:  ', tmp_table_size, ' bytes (', ROUND(tmp_table_size/(1024*1024), 1), 'MB)
    Max heap table size: ', max_heap_table_size, ' bytes (', ROUND(max_heap_table_size/(1024*1024), 1), 'MB)
    Created:             ', created_tmp_tables_psec, '/sec
    Created disk tables: ', created_tmp_disk_tables_psec, '/sec  ', IFNULL(created_disk_tmp_tables_percent, 'N/A'), '%

Connections:
    Max connections: ', max_connections, '. Max used: ', max_used_connections, '  ',
        IFNULL(max_connections_used_percent, 'N/A'), '%
    Connections: ', connections_psec, '/sec
    Aborted:     ', aborted_connects_psec, '/sec  ', IFNULL(aborted_connections_percent, 'N/A'), '%

Threads:
    Thread cache: ', thread_cache_size, '. Used: ', IFNULL(thread_cache_used_percent, 'N/A'), '%
    Created: ', threads_created_psec, '/sec

Replication:
    Master status file number: ', IFNULL(master_status_file_number, 'N/A'), ', position: ', IFNULL(master_status_position, 'N/A'), '
    Relay log space limit: ', IFNULL(relay_log_space_limit, 'N/A'), ', used: ', IFNULL(relay_log_space, 'N/A'), '  (',
        IFNULL(relay_log_space_used_percent, 'N/A'), '%)
    Seconds behind master: ', IFNULL(seconds_behind_master, 'N/A'), '
    Estimated time for slave to catch up: ', IFNULL(IF(seconds_behind_master_psec >= 0, NULL, FLOOR(-seconds_behind_master/seconds_behind_master_psec)), 'N/A'), ' seconds (',
        IFNULL(FLOOR(IF(seconds_behind_master_psec >= 0, NULL, -seconds_behind_master/seconds_behind_master_psec)/(60*60*24)), 'N/A'), ' days, ',
        IFNULL(SEC_TO_TIME(IF(seconds_behind_master_psec >= 0, NULL, -seconds_behind_master/seconds_behind_master_psec) % (60*60*24)), 'N/A'), ' hours)  ETA: ',
        IFNULL(TIMESTAMP(ts) + INTERVAL estimated_slave_catchup_seconds SECOND, 'N/A'), '
') AS report
          FROM
            ${database_name}.sv_report_${view_name_extension}
    """
    query = query.replace("${database_name}", database_name)

    for view_name_extension in ["sample", "hour", "day"]:
        custom_query = query.replace("${view_name_extension}", view_name_extension)
        act_query(custom_query)

    verbose("report human views created")


def create_report_24_7_view():
    """
    Generate a 24/7 report view
    """

    all_columns = custom_views_columns["report"]
    columns_listing = ",\n".join(["AVG(%s) AS %s" % (column_name, column_name,) for column_name in all_columns])

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_24_7 AS
          SELECT
            NULL AS ts,
            WEEKDAY(ts) AS wd,
            HOUR(ts) AS hr,
            %s
          FROM
            ${database_name}.sv_report_sample
          GROUP BY WEEKDAY(ts), HOUR(ts)
          ORDER BY WEEKDAY(ts), HOUR(ts)
        """ % (columns_listing)
    query = query.replace("${database_name}", database_name)

    act_query(query)

    verbose("24/7 report view created")



def generate_google_chart_24_7_query(chart_column):

    chart_color = "4682b4"

    query = """
          REPLACE(
          CONCAT(
            charts_api.service_url, '?cht=s&chs=', charts_api.chart_width, 'x', charts_api.chart_height,
            '&chts=303030,12&chtt=${chart_column}&chd=t:',
            CONCAT_WS('|',
              GROUP_CONCAT(ROUND(hr*100/23)),
              GROUP_CONCAT(ROUND(wd*100/6)),
              GROUP_CONCAT(ROUND(
                100*(${chart_column} - LEAST(0, ${chart_column}_min))/(${chart_column}_max - LEAST(0, ${chart_column}_min))
                ))
            ),
            '&chxt=x,y&chxl=0:|00|01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|1:|Mon|Tue|Wed|Thu|Fri|Sat|Sun',
            '&chm=o,${chart_color},0,-1,18,0'
          ), ' ', '+') AS ${chart_column}
        """
    query = query.replace("${chart_column}", chart_column)
    query = query.replace("${chart_color}", chart_color)

    return query


def create_report_google_chart_24_7_view(charts_list):
    charts_queries = [generate_google_chart_24_7_query(chart_column) for chart_column in charts_list]
    charts_query = ",".join(charts_queries)
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_chart_24_7 AS
          SELECT
            %s
          FROM
            ${database_name}.sv_report_24_7, ${database_name}.sv_report_24_7_minmax, ${database_name}.charts_api
        """ % charts_query
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("report 24/7 chart view created")


def create_report_html_24_7_view(report_columns):
    row_queries = []
    while report_columns:
        current_row_chart_queries = []
        current_row_report_columns = report_columns[0:3]
        report_columns = report_columns[3:] 

        for report_column in current_row_report_columns:
            query = """
                '<div class="chart">
                    <h3>%s</h3>',
                    IFNULL(CONCAT('<img src="', %s, '"/>'), 'N/A'),
                '</div>',
                """ % (report_column.replace("_", " "), report_column)
            current_row_chart_queries.append(query)

        row_query = """'
            
            <div class="row">',
                %s
                '<div class="clear"></div>
            </div>
                ',
            """ % ("".join(current_row_chart_queries))
        row_queries.append(row_query)

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_html_24_7 AS
          SELECT CONCAT('
            <html>
                <head>
                <title>mycheckpoint 24/7 report</title>
                <meta http-equiv="refresh" content="600" />
                <style type="text/css">
                    body {
                        background:#FFFFFF none repeat scroll 0% 0%;
                        color:#505050;
                        font-family:Verdana,Arial,Helvetica,sans-serif;
                        font-size:9pt;
                        line-height:1.5;
                    }
                    a {
                        color:#f26522;
                        text-decoration:none;
                    }
                    h3 {
                        font-weight:normal;
                    }
                    div.row {
                        width: ${global_width};
                    }
                    div.chart {
                        float: left;
                        white-space: nowrap;
                        margin-right: 10px;
                        width:', charts_api.chart_width, ';
                    }
                    div.chart img {
                        border:0px none;
                        width: ', charts_api.chart_width, ';
                        height: ', charts_api.chart_height, ';
                    }
                </style>
                </head>
                <body>
                    <a name=""></a>
                    24/7 report generated by <a href="http://code.openark.org/forge/mycheckpoint" target="mycheckpoint">mycheckpoint</a> on ',
                        DATE_FORMAT(NOW(),'%%b %%D %%Y, %%H:%%i'), '
                    <br/><br/>
                    ',
                    %s '
                </body>
            </html>
          ') AS html
          FROM
            ${database_name}.sv_report_chart_24_7, ${database_name}.charts_api
        """ % "".join(row_queries)
    query = query.replace("${database_name}", database_name)
    query = query.replace("${global_width}", str(options.chart_width*3 + 30))

    act_query(query)

    verbose("sv_report_html_24_7 created")


def create_report_recent_views():
    """
    Generate per-sample, per-hour and per-day 'recent' views, which only list the latest rows from respective full views.
    """
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = MERGE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_${view_name_extension}_recent AS
          SELECT *
          FROM
            ${database_name}.sv_report_${view_name_extension}, ${database_name}.sv_latest
          WHERE
            ts >= ${recent_timestamp}
        """
    query = query.replace("${database_name}", database_name)

    # In favour of charts, we round minutes in 10-min groups (e.g. 12:00, 12:10, 12:20, ...), and we therefroe
    # may include data slightly more than 24 hours ago.
    # With hour/day reports there's no such problem since nothing is to be rounded.
    recent_timestamp_map = {
        "sample": "ts_latest - INTERVAL SECOND(ts_latest) SECOND - INTERVAL (MINUTE(ts_latest) MOD 10) MINUTE - INTERVAL 24 HOUR",
        "hour": "ts_latest - INTERVAL 10 DAY",
        "day": "ts_latest - INTERVAL 1 YEAR",
        }
    for view_name_extension in recent_timestamp_map:
        custom_query = query
        custom_query = custom_query.replace("${view_name_extension}", view_name_extension)
        custom_query = custom_query.replace("${recent_timestamp}", recent_timestamp_map[view_name_extension])
        act_query(custom_query)

    verbose("recent reports views created")


def create_report_sample_recent_aggregated_view():
    all_columns = custom_views_columns["report"]
    columns_listing = ",\n".join(["AVG(%s) AS %s" % (column_name, column_name,) for column_name in all_columns])
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_sample_recent_aggregated AS
          SELECT
            MAX(id) AS id,
            ts
              - INTERVAL SECOND(ts) SECOND
              - INTERVAL (MINUTE(ts) %% 10) MINUTE
              AS ts,
            %s
          FROM
            ${database_name}.sv_report_sample_recent
          GROUP BY
            ts
              - INTERVAL SECOND(ts) SECOND
              - INTERVAL (MINUTE(ts) %% 10) MINUTE
        """ % (columns_listing)
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_report_sample_recent_aggregated view created")


def create_report_minmax_views():
    """
    Generate min/max values view for the report views.
    These are used by the chart labels views and the chart views.
    """

    all_columns = custom_views_columns["report"]

    min_columns_listing = ",\n".join(["MIN(%s) AS %s_min" % (column_name, column_name,) for column_name in all_columns])
    max_columns_listing = ",\n".join(["MAX(%s) AS %s_max" % (column_name, column_name,) for column_name in all_columns])

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_${view_name_extension}_minmax AS
          SELECT
            COUNT(*) AS count_rows,
            MIN(ts) AS ts_min,
            MAX(ts) AS ts_max,
            TIMESTAMPDIFF(SECOND, MIN(TS), MAX(ts)) AS ts_diff_seconds,
            %s,
            %s
          FROM
            ${database_name}.sv_report_${input_view_extension}
        """ % (min_columns_listing, max_columns_listing)
    query = query.replace("${database_name}", database_name)

    input_views_extensions = {
        "sample_recent": "sample_recent_aggregated",
        "hour_recent":   "hour_recent",
        "day_recent":    "day_recent",
        "24_7":    "24_7",
        }
    for view_name_extension in input_views_extensions:
        input_view_extension = input_views_extensions[view_name_extension]
        custom_query = query
        custom_query = custom_query.replace("${input_view_extension}", input_view_extension)
        custom_query = custom_query.replace("${view_name_extension}", view_name_extension)
        act_query(custom_query)

    verbose("reports minmax views created")


def create_report_chart_sample_timeseries_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_chart_sample_timeseries AS
          SELECT
            ts_min
              - INTERVAL SECOND(ts_min) SECOND
              - INTERVAL (MINUTE(ts_min) % 10) MINUTE
              + INTERVAL (numbers.n*10) MINUTE
              AS timeseries_ts,
            numbers.n AS timeseries_key,
            sv_report_sample_recent_aggregated.*
          FROM
            ${database_name}.numbers
            JOIN ${database_name}.sv_report_sample_recent_minmax
            LEFT JOIN ${database_name}.sv_report_sample_recent_aggregated ON (
              ts_min
                - INTERVAL SECOND(ts_min) SECOND
                - INTERVAL (MINUTE(ts_min) % 10) MINUTE
                + INTERVAL (numbers.n*10) MINUTE
              = ts
            )
          WHERE
            numbers.n <= TIMESTAMPDIFF(MINUTE, ts_min, ts_max)/10 + 1
            AND ts_min
              - INTERVAL SECOND(ts_min) SECOND
              - INTERVAL (MINUTE(ts_min) % 10) MINUTE
              + INTERVAL (numbers.n*10) MINUTE <= ts_max
        """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_report_chart_sample_timeseries view created")


def create_report_chart_hour_timeseries_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_chart_hour_timeseries AS
          SELECT
            ts_min
              + INTERVAL numbers.n HOUR
              AS timeseries_ts,
            numbers.n AS timeseries_key,
            sv_report_hour_recent.*
          FROM
            ${database_name}.numbers
            JOIN ${database_name}.sv_report_hour_recent_minmax
            LEFT JOIN ${database_name}.sv_report_hour_recent ON (
              ts_min
                + INTERVAL numbers.n HOUR
              = ts
            )
          WHERE
            numbers.n <= TIMESTAMPDIFF(HOUR, ts_min, ts_max) + 1
            AND ts_min
              + INTERVAL numbers.n HOUR <= ts_max
        """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_report_chart_hour_timeseries view created")


def create_report_chart_day_timeseries_view():
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_chart_day_timeseries AS
          SELECT
            ts_min
              + INTERVAL numbers.n DAY
              AS timeseries_ts,
            numbers.n AS timeseries_key,
            sv_report_day_recent.*
          FROM
            ${database_name}.numbers
            JOIN ${database_name}.sv_report_day_recent_minmax
            LEFT JOIN ${database_name}.sv_report_day_recent ON (
              ts_min
                + INTERVAL numbers.n DAY
              = ts
            )
          WHERE
            numbers.n <= TIMESTAMPDIFF(DAY, ts_min, ts_max) + 1
            AND ts_min
              + INTERVAL numbers.n DAY <= ts_max
        """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_report_chart_day_timeseries view created")


def create_report_chart_labels_views():
    """
    Generate x-axis labels for the google api report views
    """

    title_ts_formats = {
        "sample": "%b %e, %H:%i",
        "hour":   "%b %e, %H:00",
        "day":    "%b %e, %Y",
        }
    title_descriptions = {
        "sample": ("ROUND(TIMESTAMPDIFF(MINUTE, ts_min, ts_max)/60)", "hours"),
        "hour":   ("ROUND(TIMESTAMPDIFF(HOUR, ts_min, ts_max)/24)", "days"),
        "day":    ("ROUND(TIMESTAMPDIFF(HOUR, ts_min, ts_max)/24)", "days"),
        }
    ts_formats = {
        "sample": "%H:00",
        "hour":   "%D",
        "day":    "%b %e",
        }
    labels_times = {
        "sample": ("DATE(ts_min) + INTERVAL HOUR(ts_min) HOUR", "HOUR"),
        "hour":   ("DATE(ts_min)", "DAY"),
        "day":    ("DATE(ts_min) - INTERVAL WEEKDAY(ts_min) DAY", "WEEK"),
        }
    labels_step_and_limits = {
        "sample": ("HOUR", 4, 24),
        "hour":   ("DAY", 1, 10),
        "day":    ("DAY", 1, 52),
        }
    x_axis_map = {
        "sample": ("ROUND(60*100/TIMESTAMPDIFF(MINUTE, ts_min, ts_max), 2)", "ROUND(((60 - MINUTE(ts_min)) MOD 60)*100/TIMESTAMPDIFF(MINUTE, ts_min, ts_max), 2)"),
        "hour":   ("ROUND(24*100/TIMESTAMPDIFF(HOUR, ts_min, ts_max), 2)", "ROUND(((24 - HOUR(ts_min)) MOD 24)*100/TIMESTAMPDIFF(HOUR, ts_min, ts_max) ,2)"),
        "day":    ("ROUND(7*100/TIMESTAMPDIFF(DAY, ts_min, ts_max), 2)", "ROUND(((7 - WEEKDAY(ts_min)) MOD 7)*100/TIMESTAMPDIFF(DAY, ts_min, ts_max) ,2)"),
        }
    stale_error_conditions = {
        "sample": "ts_max < NOW() - INTERVAL 1 HOUR",
        "hour":   "ts_max < NOW() - INTERVAL 2 HOUR",
        "day":    "ts_max < NOW() - INTERVAL 1 DAY",
        }

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_chart_${view_name_extension}_labels AS
          SELECT
            IFNULL(${x_axis_step_size}, '') AS x_axis_step_size,
            IFNULL(${x_axis_offset}, '') AS x_axis_offset,
            IFNULL(
              GROUP_CONCAT(
                IF(${label_function}(${base_ts} + INTERVAL numbers.n ${interval_unit}) % ${labels_step} = 0,
                  LOWER(DATE_FORMAT(${base_ts} + INTERVAL numbers.n ${interval_unit}, '${ts_format}')),
                  IF(${label_function}(${base_ts} + INTERVAL numbers.n ${interval_unit}) % ${labels_step} = ${labels_step}/2,
                    ' ', ''
                  )
                )
                SEPARATOR '|'),
              '') AS x_axis_labels,
            IFNULL(
              GROUP_CONCAT(
                ${x_axis_offset} + (${x_axis_step_size})*IF(${base_ts} < ts_min, n-1, n)
                SEPARATOR ','),
              '') AS x_axis_labels_positions,
            CONCAT(IF (${stale_error_condition}, 'STALE DATA! ', 'Latest '), ${title_numeric_description}, ' ${title_unit_description}: ',
              DATE_FORMAT(ts_min, '${title_ts_format}'), '  -  ', DATE_FORMAT(ts_max, '${title_ts_format}')) AS chart_time_description,
            IF (${stale_error_condition}, '808080', '303030') AS chart_title_color,
            IF (${stale_error_condition}, 'fff88f', 'ffffff') AS chart_bg_color
          FROM
            ${database_name}.sv_report_${view_name_extension}_recent_minmax, ${database_name}.numbers
          WHERE
            ${base_ts} + INTERVAL numbers.n ${interval_unit} >= ts_min
            AND ${base_ts} + INTERVAL numbers.n ${interval_unit} <= ts_max
            AND numbers.n <= ${labels_limit}
        """
    query = query.replace("${database_name}", database_name)

    for view_name_extension in ["sample", "hour", "day"]:
        title_ts_format = title_ts_formats[view_name_extension]
        title_numeric_description, title_unit_description = title_descriptions[view_name_extension]
        base_ts, interval_unit = labels_times[view_name_extension]
        ts_format = ts_formats[view_name_extension]
        label_function, labels_step, labels_limit = labels_step_and_limits[view_name_extension]
        x_axis_step_size, x_axis_offset = x_axis_map[view_name_extension]
        stale_error_condition = stale_error_conditions[view_name_extension]
        custom_query = query
        custom_query = custom_query.replace("${view_name_extension}", view_name_extension)
        custom_query = custom_query.replace("${base_ts}", base_ts)
        custom_query = custom_query.replace("${title_ts_format}", title_ts_format)
        custom_query = custom_query.replace("${title_numeric_description}", title_numeric_description)
        custom_query = custom_query.replace("${title_unit_description}", title_unit_description)
        custom_query = custom_query.replace("${interval_unit}", interval_unit)
        custom_query = custom_query.replace("${ts_format}", str(ts_format))
        custom_query = custom_query.replace("${labels_step}", str(labels_step))
        custom_query = custom_query.replace("${label_function}", label_function)
        custom_query = custom_query.replace("${labels_limit}", str(labels_limit))
        custom_query = custom_query.replace("${x_axis_step_size}", str(x_axis_step_size))
        custom_query = custom_query.replace("${x_axis_offset}", str(x_axis_offset))
        custom_query = custom_query.replace("${stale_error_condition}", stale_error_condition)
        act_query(custom_query)

    verbose("report charts labels views created")


def generate_google_chart_query(chart_columns, alias, scale_from_0=False, scale_to_100=False):
    chart_columns_list = [column_name.strip() for column_name in chart_columns.lower().split(",")]

    chart_column_min_listing = ",".join(["%s_min" % column_name for column_name in chart_columns_list])
    chart_column_max_listing = ",".join(["%s_max" % column_name for column_name in chart_columns_list])

    if scale_from_0:
        least_value_clause = "LEAST(0,%s)" % chart_column_min_listing
    elif len(chart_columns_list) > 1:
        least_value_clause = "LEAST(%s)" % chart_column_min_listing
    else:
        # Sadly, LEAST doesn;t work for 1 argument only... So we need a special case here
        least_value_clause = chart_column_min_listing

    if scale_to_100:
        greatest_value_clause = "GREATEST(100,%s)" % chart_column_max_listing
    elif len(chart_columns_list) > 1:
        greatest_value_clause = "GREATEST(%s)" % chart_column_max_listing
    else:
        # Sadly, LEAST doesn;t work for 1 argument only... So we need a special case here
        greatest_value_clause = chart_column_max_listing


    piped_chart_column_listing = "|".join(chart_columns_list)

    chart_colors = ["ff8c00", "4682b4", "9acd32", "dc143c", "9932cc", "ffd700", "191970", "7fffd4", "808080", "dda0dd"][0:len(chart_columns_list)]

    # '_' is used for missing (== NULL) values.
    column_values = [ """
        GROUP_CONCAT(
          IFNULL(
            SUBSTRING(
              charts_api.simple_encoding,
              1+ROUND(
                61 *
                (%s - IFNULL(${least_value_clause}, 0))/(IFNULL(${greatest_value_clause}, 0) - IFNULL(${least_value_clause}, 0))
              )
            , 1)
          , '_')
          ORDER BY timeseries_key ASC
          SEPARATOR ''
        ),
        """ % (column_name) for column_name in chart_columns_list
    ]
    concatenated_column_values = "',',".join(column_values)

    query = """
          REPLACE(
          CONCAT(
            charts_api.service_url, '?cht=lc&chs=', charts_api.chart_width, 'x', charts_api.chart_height, '&chts=', chart_title_color, ',12&chtt=',
            chart_time_description, '&chf=c,s,', chart_bg_color,
            '&chdl=${piped_chart_column_listing}&chdlp=b&chco=${chart_colors}&chd=s:', ${concatenated_column_values}
            '&chxt=x,y&chxr=1,', ${least_value_clause},',', ${greatest_value_clause}, '&chxl=0:|', x_axis_labels, '|&chxs=0,505050,10,0,lt',
            '&chg=', x_axis_step_size, ',25,1,2,', x_axis_offset, ',0',
            '&chxp=0,', x_axis_labels_positions
          ), ' ', '+') AS ${alias}
        """
    query = query.replace("${database_name}", database_name)
    query = query.replace("${piped_chart_column_listing}", piped_chart_column_listing)
    query = query.replace("${chart_colors}", ",".join(chart_colors))
    query = query.replace("${concatenated_column_values}", concatenated_column_values)
    query = query.replace("${least_value_clause}", least_value_clause)
    query = query.replace("${greatest_value_clause}", greatest_value_clause)
    query = query.replace("${alias}", alias)

    return query


def create_report_google_chart_views(charts_list):
    for view_name_extension in ["sample", "hour", "day"]:
        charts_queries = [generate_google_chart_query(chart_columns, alias, scale_from_0, scale_to_100) for (chart_columns, alias, scale_from_0, scale_to_100) in charts_list]
        charts_query = ",".join(charts_queries)
        query = """
            CREATE
            OR REPLACE
            ALGORITHM = TEMPTABLE
            DEFINER = CURRENT_USER
            SQL SECURITY INVOKER
            VIEW ${database_name}.sv_report_chart_${view_name_extension} AS
              SELECT
                %s
              FROM
                ${database_name}.sv_report_chart_${view_name_extension}_timeseries, ${database_name}.sv_report_${view_name_extension}_recent_minmax, ${database_name}.charts_api, ${database_name}.sv_report_chart_${view_name_extension}_labels
            """ % charts_query
        query = query.replace("${database_name}", database_name)
        query = query.replace("${view_name_extension}", view_name_extension)
        act_query(query)

    verbose("report charts views created")



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


def create_report_html_view(charts_aliases):
    charts_aliases_list = [chart_alias.strip() for chart_alias in charts_aliases.split(",")]

    all_img_tags_queries = []
    for chart_alias in charts_aliases_list:
        alias_img_tags_query = """
            '<div class="row">
                <a name="${chart_alias}"></a>
                <h2>${chart_alias} <a href="#">[top]</a></h2>',
                '<div class="chart">', IFNULL(CONCAT('<img src="', sv_report_chart_sample.${chart_alias}, '"/>'), 'N/A'), '</div>',
                '<div class="chart">', IFNULL(CONCAT('<img src="', sv_report_chart_hour.${chart_alias}, '"/>'), 'N/A'), '</div>',
                '<div class="chart">', IFNULL(CONCAT('<img src="', sv_report_chart_day.${chart_alias}, '"/>'), 'N/A'), '</div>',
                '<div class="clear"></div>',
            '</div>
                ',
            """
        alias_img_tags_query = alias_img_tags_query.replace("${chart_alias}", chart_alias)
        all_img_tags_queries.append(alias_img_tags_query)
    all_img_tags_query = "".join(all_img_tags_queries)

    chart_aliases_map = " | ".join(["""<a href="#%s">%s</a>""" % (chart_alias, chart_alias,) for chart_alias in charts_aliases_list])
    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_report_html AS
          SELECT CONCAT('
    <html>
        <head>
        <title>mycheckpoint report</title>
        <meta http-equiv="refresh" content="600" />
        <style type="text/css">
            body {
                background:#FFFFFF none repeat scroll 0% 0%;
                color:#505050;
                font-family:Verdana,Arial,Helvetica,sans-serif;
                font-size:9pt;
                line-height:1.5;
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
            .nobr {
                white-space: nowrap;
            }
            div.row {
                width: ${global_width};
            }
            div.chart {
                float: left;
                white-space: nowrap;
                margin-right: 10px;
                width:', charts_api.chart_width, ';
            }
            div.chart img {
                border:0px none;
                width: ', charts_api.chart_width, ';
                height: ', charts_api.chart_height, ';
            }
            .clear {
                clear:both;
                height:1px;
            }
        </style>
        </head>
        <body>
            Report generated by <a href="http://code.openark.org/forge/mycheckpoint" target="mycheckpoint">mycheckpoint</a> on ',
                DATE_FORMAT(NOW(),'%%b %%D %%Y, %%H:%%i'), '
            <br/><br/>
            Navigate: ${chart_aliases_map}
            <br/>
            ',
            %s '
        </body>
    </html>
          ') AS html
          FROM
            ${database_name}.sv_report_chart_sample, ${database_name}.sv_report_chart_hour, ${database_name}.sv_report_chart_day, ${database_name}.charts_api
        """ % all_img_tags_query
    query = query.replace("${database_name}", database_name)
    query = query.replace("${chart_aliases_map}", chart_aliases_map)
    query = query.replace("${global_width}", str(options.chart_width*3 + 30))
    act_query(query)

    verbose("report html view created")


def create_report_html_brief_view(report_charts):
    charts_sections_list = [chart_section for (chart_section, charts_aliases) in report_charts]
    chart_aliases_navigation_map = " | ".join(["""<a href="#%s">%s</a>""" % (chart_section, chart_section) for chart_section in charts_sections_list if chart_section])

    sections_queries = []
    for (chart_section, charts_aliases) in report_charts:
        charts_aliases_list = [chart_alias.strip() for chart_alias in charts_aliases.split(",")]
        charts_aliases_queries = []
        for chart_alias in charts_aliases_list:
            query = """
                '<div class="chart">
                    <h3>%s</h3>',
                    IFNULL(CONCAT('<img src="', %s, '"/>'), 'N/A'),
                '</div>',
                """ % (chart_alias.replace("_", " "), chart_alias)
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
        VIEW ${database_name}.sv_report_html_brief AS
          SELECT CONCAT('
            <html>
                <head>
                <title>mycheckpoint brief report</title>
                <meta http-equiv="refresh" content="600" />
                <style type="text/css">
                    body {
                        background:#FFFFFF none repeat scroll 0% 0%;
                        color:#505050;
                        font-family:Verdana,Arial,Helvetica,sans-serif;
                        font-size:9pt;
                        line-height:1.5;
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
                    h3 {
                        font-weight:normal;
                    }
                    .nobr {
                        white-space: nowrap;
                    }
                    div.row {
                        width: ${global_width};
                    }
                    div.chart {
                        float: left;
                        white-space: nowrap;
                        margin-right: 10px;
                        width:', charts_api.chart_width, ';
                    }
                    div.chart img {
                        border:0px none;
                        width: ', charts_api.chart_width, ';
                        height: ', charts_api.chart_height, ';
                    }
                    .clear {
                        clear:both;
                        height:1px;
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
            ${database_name}.sv_report_chart_sample, ${database_name}.charts_api
        """ % "".join(sections_queries)
    query = query.replace("${database_name}", database_name)
    query = query.replace("${chart_aliases_navigation_map}", chart_aliases_navigation_map)
    query = query.replace("${global_width}", str(options.chart_width*3 + 30))

    act_query(query)

    verbose("sv_report_html_brief created")


def create_report_html_brief_interactive_view(report_charts):
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


def create_status_variables_parameter_change_view():
    global_variables, _diff_columns = get_variables_and_status_columns()

    global_variables_select_listing = ["""
        SELECT ${status_variables_table_alias}2.ts AS ts, '%s' AS variable_name, ${status_variables_table_alias}1.%s AS old_value, ${status_variables_table_alias}2.%s AS new_value
        FROM
          ${database_name}.${status_variables_table_name} AS ${status_variables_table_alias}1
          INNER JOIN ${database_name}.${status_variables_table_name} AS ${status_variables_table_alias}2
          ON (${status_variables_table_alias}1.id = ${status_variables_table_alias}2.id-GREATEST(1, IFNULL(${status_variables_table_alias}2.auto_increment_increment, 1)))
        WHERE ${status_variables_table_alias}2.%s != ${status_variables_table_alias}1.%s
        """ % (column_name, column_name, column_name,
               column_name, column_name,) for column_name in global_variables if column_name != 'timestamp']
    global_variables_select_union = " UNION ALL \n".join(global_variables_select_listing)

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_parameter_change_union AS
          %s
    """ % (global_variables_select_union,)
    query = query.replace("${database_name}", database_name)
    query = query.replace("${status_variables_table_name}", table_name)
    query = query.replace("${status_variables_table_alias}", table_name)
    act_query(query)

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_param_change AS
          SELECT *
          FROM ${database_name}.sv_parameter_change_union
          ORDER BY ts, variable_name
    """
    query = query.replace("${database_name}", database_name)
    act_query(query)

    verbose("sv_param_change view created")


def create_status_variables_long_format_view():
    global_variables, status_variables = get_variables_and_status_columns()
    all_columns_listing = []
    all_columns_listing.extend(global_variables);
    all_columns_listing.extend(status_variables);
    all_columns_listing.extend(["%s_diff" % (column_name,) for column_name in status_variables])
    all_columns_listing.extend(["%s_psec" % (column_name,) for column_name in status_variables])
    all_columns = ",".join(all_columns_listing)

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_long_hour AS
            SELECT
                id, ts,
                SUBSTRING_INDEX(SUBSTRING_INDEX('%s', ',', numbers.n), ',', -1) AS variable_name,
                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT_WS(',', %s), ',', numbers.n), ',', -1) AS UNSIGNED) AS variable_value
            FROM
                ${database_name}.sv_hour,
                ${database_name}.numbers
            WHERE
                numbers.n >= 1 AND numbers.n <= %d
            ORDER BY
                id ASC, variable_name ASC
        """ % (all_columns, all_columns, len(all_columns_listing))
    query = query.replace("${database_name}", database_name)
    act_query(query)


def create_status_variables_aggregated_view():
    global_variables, status_variables = get_variables_and_status_columns()
    all_columns_listing = []
    all_columns_listing.extend(global_variables);
    all_columns_listing.extend(status_variables);
    all_columns = ",".join(all_columns_listing)

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = TEMPTABLE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_agg_hour AS
            SELECT
                MIN(id) AS id, MIN(ts) AS min_ts, MAX(ts) AS max_ts,
                SUBSTRING_INDEX(SUBSTRING_INDEX('%s', ',', numbers.n), ',', -1) AS variable_name,
                GROUP_CONCAT(CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT_WS(',', %s), ',', numbers.n), ',', -1) AS UNSIGNED) ORDER BY ts ASC) AS variable_values
            FROM
                ${database_name}.sv_hour,
                ${database_name}.numbers
            WHERE
                numbers.n >= 1 AND numbers.n <= %d
            GROUP BY
                variable_name ASC
        """ % (all_columns, all_columns, len(all_columns_listing))
    query = query.replace("${database_name}", database_name)
    act_query(query)


def create_custom_views(view_base_name, view_columns, custom_columns = ""):
    global_variables, status_variables = get_variables_and_status_columns()

    view_columns_list = [column_name.strip() for column_name in view_columns.lower().split(",")]
    all_columns_listing = []
    all_columns_listing.extend([column_name for column_name in view_columns_list if column_name in global_variables])
    all_columns_listing.extend([column_name for column_name in view_columns_list if column_name in status_variables])
    all_columns_listing.extend([" %s_diff" % (column_name,) for column_name in view_columns_list if column_name in status_variables])
    all_columns_listing.extend(["%s_psec" % (column_name,) for column_name in view_columns_list if column_name in status_variables])
    columns_listing = ",\n".join(all_columns_listing)
    if custom_columns:
        if view_columns:
            columns_listing = columns_listing + ",\n "
        columns_listing = columns_listing + custom_columns

    # This is currently an ugly patch (first one in this code...)
    # We need to know which custom columns have been created in the "report" views, so that we can later build the
    # sv_report_minmax_* views.
    # So we parse the custom columns. We expect one column per line; we allow for aliasing (" as ")
    # We then store the columns for later use.
    custom_columns_names_list = [column_name for column_name in custom_columns.lower().split("\n")]
    custom_columns_names_list = [column_name.split(" as ")[-1].replace(",","").strip() for column_name in custom_columns_names_list]
    custom_columns_names_list = [column_name for column_name in custom_columns_names_list if column_name]
    custom_views_columns[view_base_name] = custom_columns_names_list

    query = """
        CREATE
        OR REPLACE
        ALGORITHM = MERGE
        DEFINER = CURRENT_USER
        SQL SECURITY INVOKER
        VIEW ${database_name}.sv_%s_${view_name_extension} AS
          SELECT
            id,
            ts,
            ts_diff_seconds,
            %s
          FROM
            ${database_name}.sv_${view_name_extension}
    """ % (view_base_name,
           columns_listing)
    query = query.replace("${database_name}", database_name)

    for view_name_extension in ["sample", "hour", "day"]:
        custom_query = query.replace("${view_name_extension}", view_name_extension)
        act_query(custom_query)

    verbose("%s custom views created" % view_base_name)


def create_status_variables_views():
    # General status variables views:
    create_status_variables_latest_view()
    create_status_variables_diff_view()
    create_status_variables_sample_view()
    create_status_variables_hour_view()
    create_status_variables_day_view()
    create_status_variables_parameter_change_view()

    # Report views:
    create_custom_views("report", "", """
            uptime,
            LEAST(100, ROUND(100*uptime_diff/NULLIF(ts_diff_seconds, 0), 1)) AS uptime_percent,

            innodb_buffer_pool_size,
            innodb_flush_log_at_trx_commit,
            ROUND(100 - 100*innodb_buffer_pool_pages_free/NULLIF(innodb_buffer_pool_pages_total, 0), 1) AS innodb_buffer_pool_used_percent,
            ROUND(100 - (100*innodb_buffer_pool_reads_diff/NULLIF(innodb_buffer_pool_read_requests_diff, 0)), 2) AS innodb_read_hit_percent,
            innodb_buffer_pool_reads_psec,
            innodb_buffer_pool_pages_flushed_psec,
            innodb_os_log_written_psec,
            ROUND(innodb_os_log_written_psec*60*60/1024/1024, 1) AS innodb_estimated_log_mb_written_per_hour,
            innodb_row_lock_waits_psec,
            innodb_row_lock_current_waits,

            bytes_sent_psec/1024/1024 AS mega_bytes_sent_psec,
            bytes_received_psec/1024/1024 AS mega_bytes_received_psec,

            key_buffer_size,
            key_reads_diff,
            key_read_requests_diff,
            key_writes_diff,
            key_write_requests_diff,
            key_reads_psec,
            key_read_requests_psec,
            key_writes_psec,
            key_write_requests_psec,
            key_read_requests_psec - key_reads_psec AS key_read_hits_psec, 
            key_write_requests_psec - key_writes_psec AS key_write_hits_psec, 
            ROUND(100 - 100*(key_blocks_unused * key_cache_block_size)/NULLIF(key_buffer_size, 0), 1) AS key_buffer_used_percent,
            ROUND(100 - 100*key_reads_diff/NULLIF(key_read_requests_diff, 0), 1) AS key_read_hit_percent,
            ROUND(100 - 100*key_writes_diff/NULLIF(key_write_requests_diff, 0), 1) AS key_write_hit_percent,

            com_select_psec,
            com_insert_psec,
            com_update_psec,
            com_delete_psec,
            com_replace_psec,
            com_set_option_psec,
            com_commit_psec,
            slow_queries_psec,
            questions_psec,
            queries_psec,
            ROUND(100*com_select_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_select_percent,
            ROUND(100*com_insert_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_insert_percent,
            ROUND(100*com_update_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_update_percent,
            ROUND(100*com_delete_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_delete_percent,
            ROUND(100*com_replace_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_replace_percent,
            ROUND(100*com_set_option_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_set_option_percent,
            ROUND(100*com_commit_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS com_commit_percent,
            ROUND(100*slow_queries_diff/NULLIF(IFNULL(queries_diff, questions_diff), 0), 1) AS slow_queries_percent,
            long_query_time,

            select_scan_psec,
            select_full_join_psec,
            select_range_psec,
            ROUND(100*select_scan_diff/NULLIF(com_select_diff, 0), 1) AS select_scan_percent,
            ROUND(100*select_full_join_diff/NULLIF(com_select_diff, 0), 1) AS select_full_join_percent,
            ROUND(100*select_range_diff/NULLIF(com_select_diff, 0), 1) AS select_range_percent,
            sort_merge_passes_psec,

            table_locks_waited_psec,
            ROUND(100*table_locks_waited_diff/NULLIF(table_locks_waited_diff + table_locks_immediate_diff, 0), 1) AS table_lock_waited_percent,

            IFNULL(table_cache, 0) + IFNULL(table_open_cache, 0) AS table_cache_size,
            open_tables,
            ROUND(100*open_tables/NULLIF(IFNULL(table_cache, 0) + IFNULL(table_open_cache, 0), 0), 1) AS table_cache_use_percent,
            opened_tables_psec,

            tmp_table_size,
            max_heap_table_size,
            created_tmp_tables_psec,
            created_tmp_disk_tables_psec,
            ROUND(100*created_tmp_disk_tables_diff/NULLIF(created_tmp_tables_diff, 0), 1) AS created_disk_tmp_tables_percent,

            max_connections,
            max_used_connections,
            ROUND(100*max_used_connections/NULLIF(max_connections, 0), 1) AS max_connections_used_percent,
            connections_psec,
            aborted_connects_psec,
            ROUND(100*aborted_connects_diff/NULLIF(connections_diff, 0), 1) AS aborted_connections_percent,

            thread_cache_size,
            threads_cached,
            ROUND(100*threads_cached/NULLIF(thread_cache_size, 0), 1) AS thread_cache_used_percent,
            threads_created_psec,
            threads_connected,
            ROUND(100*threads_connected/NULLIF(max_connections, 0), 1) AS threads_connected_used_percent,
            threads_running,

            master_status_file_number,
            master_status_position,
            relay_log_space_limit,
            relay_log_space_limit/1024/1024 AS relay_log_space_limit_mb,
            max_relay_log_size,
            IF(max_relay_log_size = 0, max_binlog_size, max_relay_log_size) AS relay_log_max_size,
            IF(max_relay_log_size = 0, max_binlog_size, max_relay_log_size)/1024/1024 AS relay_log_max_size_mb,
            relay_log_space,
            relay_log_space/1024/1024 AS relay_log_space_mb,
            ROUND(100*relay_log_space/NULLIF(relay_log_space_limit, 0), 1) AS relay_log_space_used_percent,
            seconds_behind_master,
            seconds_behind_master_psec,
            IF(seconds_behind_master_psec >= 0, NULL, FLOOR(-seconds_behind_master/seconds_behind_master_psec)) AS estimated_slave_catchup_seconds,

            ROUND((os_loadavg_millis/1000), 2) AS os_loadavg,
            ROUND(100.0*(os_cpu_user_diff + os_cpu_nice_diff + os_cpu_system_diff)/(os_cpu_user_diff + os_cpu_nice_diff + os_cpu_system_diff + os_cpu_idle_diff), 1) AS os_cpu_utilization_percent,
            ROUND(os_mem_total_kb/1000, 1) AS os_mem_total_mb,
            ROUND(os_mem_free_kb/1000, 1) AS os_mem_free_mb,
            ROUND(os_mem_active_kb/1000, 1) AS os_mem_active_mb,
            ROUND((os_mem_total_kb-os_mem_free_kb)/1000, 1) AS os_mem_used_mb,
            ROUND(os_swap_total_kb/1000, 1) AS os_swap_total_mb,
            ROUND(os_swap_free_kb/1000, 1) AS os_swap_free_mb,
            ROUND((os_swap_total_kb-os_swap_free_kb)/1000, 1) AS os_swap_used_mb,

            os_root_mountpoint_usage_percent,
            os_datadir_mountpoint_usage_percent,
            os_tmpdir_mountpoint_usage_percent
        """)
    create_report_24_7_view()
    create_report_recent_views()
    create_report_sample_recent_aggregated_view()
    create_report_minmax_views()
    create_report_human_views()

    # Report chart views:
    create_report_chart_sample_timeseries_view()
    create_report_chart_hour_timeseries_view()
    create_report_chart_day_timeseries_view()
    create_report_chart_labels_views()
    report_chart_views = [
        ("uptime_percent", "uptime_percent", True, True),

        ("innodb_read_hit_percent", "innodb_read_hit_percent", False, False),
        ("innodb_buffer_pool_reads_psec, innodb_buffer_pool_pages_flushed_psec", "innodb_io", True, False),
        ("innodb_buffer_pool_used_percent", "innodb_buffer_pool_used_percent", True, True),
        ("innodb_estimated_log_mb_written_per_hour", "innodb_estimated_log_mb_written_per_hour", True, False),
        ("innodb_row_lock_waits_psec", "innodb_row_lock_waits_psec", True, False),

        ("mega_bytes_sent_psec, mega_bytes_received_psec", "bytes_io", True, False),

        ("key_buffer_used_percent", "myisam_key_buffer_used_percent", True, True),
        ("key_read_requests_psec, key_reads_psec, key_write_requests_psec, key_writes_psec", "myisam_key_hit", True, False),

        ("com_select_psec, com_insert_psec, com_delete_psec, com_update_psec, com_replace_psec", "DML", True, False),
        ("queries_psec, questions_psec, slow_queries_psec, com_commit_psec, com_set_option_psec", "questions", True, False),

        ("created_tmp_tables_psec, created_tmp_disk_tables_psec", "tmp_tables", True, False),

        ("table_locks_waited_psec", "table_locks_waited_psec", True, False),

        ("table_cache_size, open_tables", "table_cache_use", True, False),
        ("opened_tables_psec", "opened_tables_psec", True, False),

        ("connections_psec, aborted_connects_psec", "connections_psec", True, False),
        ("max_connections, threads_connected", "connections_usage", True, False),

        ("thread_cache_size, threads_cached", "thread_cache_use", True, False),
        ("threads_created_psec", "threads_created_psec", True, False),

        ("relay_log_space_limit_mb, relay_log_space_mb", "relay_log_used_mb", True, False),
        ("seconds_behind_master", "seconds_behind_master", True, True),
        ("seconds_behind_master_psec", "seconds_behind_master_psec", True, False),
        ("estimated_slave_catchup_seconds", "estimated_slave_catchup_seconds", True, False),

        ("os_cpu_utilization_percent", "os_cpu_utilization_percent", True, True),
        ("os_loadavg", "os_loadavg", True, False),
        ("os_mem_total_mb, os_mem_used_mb, os_mem_active_mb, os_swap_total_mb, os_swap_used_mb", "os_memory", True, False),

        ("os_root_mountpoint_usage_percent, os_datadir_mountpoint_usage_percent, os_tmpdir_mountpoint_usage_percent", "os_mountpoints_usage_percent", True, True),
        ]
    create_report_dygraph_chart_views(report_chart_views)
    create_report_google_chart_views(report_chart_views)
    report_24_7_columns = [
        "innodb_read_hit_percent",
        "innodb_buffer_pool_reads_psec",
        "innodb_buffer_pool_pages_flushed_psec",
        "innodb_os_log_written_psec",
        "innodb_row_lock_waits_psec",
        "mega_bytes_sent_psec",
        "mega_bytes_received_psec",
        "key_read_hit_percent",
        "key_write_hit_percent",
        "com_select_psec",
        "com_insert_psec",
        "com_delete_psec",
        "com_update_psec",
        "com_replace_psec",
        "com_set_option_percent",
        "com_commit_percent",
        "slow_queries_percent",
        "select_scan_psec",
        "select_full_join_psec",
        "select_range_psec",
        "table_locks_waited_psec",
        "opened_tables_psec",
        "created_tmp_tables_psec",
        "created_tmp_disk_tables_psec",
        "connections_psec",
        "aborted_connects_psec",
        "threads_created_psec",
        "seconds_behind_master",
        "os_loadavg",
        "os_cpu_utilization_percent",
        "os_mem_used_mb",
        "os_mem_active_mb",
        "os_swap_used_mb",
        ]
    create_report_google_chart_24_7_view(report_24_7_columns)

    # Report HTML views:
    create_report_html_24_7_view(report_24_7_columns)
    create_report_html_view("""
        innodb_read_hit_percent, innodb_io, innodb_row_lock_waits_psec, innodb_estimated_log_mb_written_per_hour, innodb_buffer_pool_used_percent,
        myisam_key_buffer_used_percent, myisam_key_hit,
        bytes_io,
        DML, questions,
        tmp_tables,
        table_locks_waited_psec,
        table_cache_use, opened_tables_psec,
        connections_psec, connections_usage,
        thread_cache_use, threads_created_psec,
        relay_log_used_mb, seconds_behind_master, seconds_behind_master_psec,
        uptime_percent,
        os_cpu_utilization_percent,
        os_loadavg,
        os_memory,
        os_mountpoints_usage_percent
        """)
    brief_html_view_charts = [
            ("InnoDB & I/O", "innodb_read_hit_percent, innodb_io, bytes_io"),
            ("Questions", "DML, questions, tmp_tables"),
            ("Resources", "connections_psec, threads_created_psec, opened_tables_psec"),
            ("Caches", "myisam_key_hit, thread_cache_use, table_cache_use"),
            ("Vitals and OS", "seconds_behind_master, os_memory, table_locks_waited_psec"),
            ("", "os_cpu_utilization_percent, os_loadavg, os_mountpoints_usage_percent"),
        ]
    create_report_html_brief_view(brief_html_view_charts)
    #create_report_html_brief_interactive_view(brief_html_view_charts)


def get_smtp_host():
    if options.smtp_host:
        return options.smtp_host
    if config.has_option(config_scope, "smtp_host"):
        return config.get(config_scope, "smtp_host")
    return "localhost"


def get_smtp_from():
    if options.smtp_from:
        return options.smtp_from
    if config.has_option(config_scope, "smtp_from"):
        return config.get(config_scope, "smtp_from")
    return "mycheckpoint@localhost"


def get_smtp_to():
    if options.smtp_to:
        return options.smtp_to.replace(" ","")
    if config.has_option(config_scope, "smtp_to"):
        return config.get(config_scope, "smtp_to").replace(" ","")
    return "mycheckpoint@localhost"


def send_email_message(description, subject, message, attachment=None):
    try:
        smtp_to = get_smtp_to()
        smtp_from = get_smtp_from()
        smtp_host = get_smtp_host()

        # Create the container (outer) email message.
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = smtp_from 
        msg["To"] = smtp_to

        message_suffix = """
    
You are receiving this email from a mycheckpoint -- MySQL monitoring utility -- installation.
Please consult your system or database administrator if you do not know why you got this mail.
-------
mycheckpoint home page: http://code.openark.org/forge/mycheckpoint
            """
        message = message + message_suffix
        msg.preamble = message
        
        if attachment:
            msg.attach(attachment)
        
        text_message = MIMEText(message)
        msg.attach(text_message)
    
        verbose("Sending %s message from %s to: %s via: %s" % (description, smtp_from, smtp_to, smtp_host))
        # Send the email via our own SMTP server.
        s = smtplib.SMTP(smtp_host)
        s.sendmail(smtp_from, smtp_to.split(","), msg.as_string())
        s.quit()
        verbose("+ Sent")
        return True
    except:
        print_error("Failed sending email")
        if options.debug:
            traceback.print_exc()
        return False


def email_brief_report():
    subject = "mycheckpoint brief report: %s" % database_name

    message = """Attached: mycheckpoint brief HTML report for database: %s""" % database_name
        
    query = "SELECT html FROM %s.sv_report_html_brief" % database_name
    brief_report = get_row(query)["html"]
    
    attachment = MIMEText(brief_report, _subtype="html")
    attachment.add_header("Content-Disposition", "attachment", filename="mycheckpoint_brief_report_%s.html" % database_name)

    send_email_message("HTML brief report", subject, message, attachment)        
            
            
def email_cannot_access_database_message():
    """
    Send an email notifying that the database cannot be reached
    """    
    if options.skip_emails:
        verbose("--skip-emails requested. Database cannot be reached; but this will not be emailed")
        return None

    email_message = """
Database alert: %s

This is an alert mail sent by mycheckpoint, monitoring your %s MySQL database.

*****************************************
mycheckpoint cannot access your database.
*****************************************
Please check:
- Is the service running?
- Are there too many connections?
- Is there a network problem?
        """ % (database_name, database_name,)
    email_subject = "%s: mycheckpoint cannot access database" % database_name
    send_email_message("cannot access", email_subject, email_message)
                

def disable_bin_log():
    if not options.disable_bin_log:
        return
    try:
        query = "SET SESSION SQL_LOG_BIN=0"
        act_query(query)
        verbose("binary logging disabled")
    except Exception:
        exit_with_error("Failed to disable binary logging. Either grant the SUPER privilege or use --skip-disable-bin-log")


def collect_status_variables():
    disable_bin_log()

    status_dict = fetch_status_variables()

    column_names = ", ".join(["%s" % column_name for column_name in sorted(status_dict.keys())])
    for column_name in status_dict.keys():
        if status_dict[column_name] is None:
            status_dict[column_name] = "NULL"
        if status_dict[column_name] == "":
            status_dict[column_name] = "NULL"
    variable_values = ", ".join(["%s" % status_dict[column_name] for column_name in sorted(status_dict.keys())])
    query = """INSERT /*! IGNORE */ INTO %s.%s
            (%s)
            VALUES (%s)
    """ % (database_name, table_name,
        column_names,
        variable_values)
    num_affected_rows = act_query(query)
    if num_affected_rows:
        verbose("New entry added")


def purge_status_variables():
    disable_bin_log()

    query = """DELETE FROM %s.%s WHERE ts < NOW() - INTERVAL %d DAY""" % (database_name, table_name, options.purge_days)
    num_affected_rows = act_query(query)
    if num_affected_rows:
        verbose("Old entries purged")
    return num_affected_rows


def purge_alert():
    """
    Since we support all storage engines, we define no foreign keys.
    After purging old records from status_variables, alert rows must be purged as well.
    """
    disable_bin_log()

    query = """
      DELETE 
        FROM ${database_name}.alert 
      WHERE 
        sv_report_sample_id < 
          (SELECT MIN(id) FROM ${database_name}.sv_report_sample)"""
    query = query.replace("${database_name}", database_name)
    num_affected_rows = act_query(query)
    if num_affected_rows:
        verbose("Old alert entries purged")
    return num_affected_rows


def deploy_schema():
    create_metadata_table()
    create_numbers_table()
    create_charts_api_table()
    if not create_status_variables_table():
        upgrade_status_variables_table()
    create_alert_condition_table()
    create_alert_table()
    create_alert_pending_table()
    create_status_variables_views()
    # Some of the following depend on sv_report_chart_sample
    create_alert_view()
    create_alert_pending_view()
    create_alert_pending_html_view()
    create_alert_email_message_items_view()
    create_alert_condition_query_view()
    verbose("Table and views deployed")


def exit_with_error(error_message):
    """
    Notify and exit.
    """
    print_error(error_message)
    sys.exit(1)


try:
    try:
        monitored_conn = None
        write_conn = None
        (options, args) = parse_options()

        # The following are overwritten by the ANT build script, and indicate
        # the revision number (e.g. SVN) and build number (e.g. timestamp)
        # In case ANT does not work for some reason, both are assumed to be 0.
        revision_placeholder = "revision.placeholder"
        if not revision_placeholder.isdigit():
            revision_placeholder = "0"
        revision_number = int(revision_placeholder)
        build_placeholder = "build.placeholder"
        if not build_placeholder.isdigit():
            build_placeholder = "0"
        build_number = int(build_placeholder)

        if options.defaults_file:
            defaults_file_name = options.defaults_file
        else:
            defaults_file_name = "/etc/mycheckpoint.cnf"
            verbose("Will assume %s as defaults file" % defaults_file_name)
        config_scope = "mycheckpoint"
        config = ConfigParser.ConfigParser()
        config.read([defaults_file_name])

        verbose("mycheckpoint rev %d, build %d. Copyright (c) 2009-2010 by Shlomi Noach" % (revision_number, build_number))

        warnings.simplefilter("ignore", MySQLdb.Warning) 
        database_name = options.database
        table_name = "status_variables"
        status_dict = {}
        extra_dict = {}
        custom_views_columns = {}
        options.chart_width = max(options.chart_width, 150)
        options.chart_height = max(options.chart_height, 100)

        # Sanity:
        if not database_name:
            exit_with_error("No database specified. Specify with -d or --database")
        if options.purge_days < 1:
            exit_with_error("purge-days must be at least 1")
        verbose("database is %s" % database_name)
        
        # Read arguments
        should_deploy = False
        should_email_brief_report = False
        for arg in args:
            if arg == "deploy":
                verbose("Deploy requested. Will deploy")
                should_deploy = True
            elif arg == "email_brief_report":
                should_email_brief_report = True
            else:
                exit_with_error("Unknown command: %s" % arg)

        # Open connections. From this point and on, database access is possible
        monitored_conn, write_conn = open_connections()
        init_connections()

        if not should_deploy:
            if not is_same_deploy():
                verbose("Non matching deployed revision. Will auto-deploy")
                should_deploy = True

        if should_deploy:
            deploy_schema()
            
        # Only take record if no arguments provided (no "command")
        if not args:
            collect_status_variables()
            if purge_status_variables():
                purge_alert()
            check_alerts()
            verbose("Status variables checkpoint complete")
        else:
            verbose("Will not monitor the database")
            
        if should_email_brief_report:
            email_brief_report()

    except Exception, err:
        if not monitored_conn:
            print_error("Cannot connect to database")
            email_cannot_access_database_message()
        print err
        if options.debug:
            traceback.print_exc()

        if "deploy" in args:
            prompt_deploy_instructions()
        else:
            prompt_collect_instructions()

        sys.exit(1)

finally:
    if monitored_conn:
        monitored_conn.close()
    if write_conn and write_conn is not monitored_conn:
        write_conn.close()

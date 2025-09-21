19.9. Run-time Statistics  
---  
[Prev](runtime-config-logging.md "19.8. Error Reporting and Logging") | [Up](runtime-config.md "Chapter 19. Server Configuration")| Chapter 19. Server Configuration| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](runtime-config-autovacuum.md "19.10. Automatic Vacuuming")  
  
* * *

## 19.9. Run-time Statistics #

[19.9.1. Cumulative Query and Index Statistics](runtime-config-statistics.md#RUNTIME-CONFIG-CUMULATIVE-STATISTICS)
[19.9.2. Statistics Monitoring](runtime-config-statistics.md#RUNTIME-CONFIG-STATISTICS-MONITOR)

### 19.9.1. Cumulative Query and Index Statistics #

These parameters control the server-wide cumulative statistics system. When enabled, the data that is collected can be accessed via the `pg_stat` and `pg_statio` family of system views. Refer to [Chapter 27](monitoring.md "Chapter 27. Monitoring Database Activity") for more information. 

`track_activities` (`boolean`)  #
    

Enables the collection of information on the currently executing command of each session, along with its identifier and the time when that command began execution. This parameter is on by default. Note that even when enabled, this information is only visible to superusers, roles with privileges of the `pg_read_all_stats` role and the user owning the sessions being reported on (including sessions belonging to a role they have the privileges of), so it should not represent a security risk. Only superusers and users with the appropriate `SET` privilege can change this setting. 

`track_activity_query_size` (`integer`)  #
    

Specifies the amount of memory reserved to store the text of the currently executing command for each active session, for the `pg_stat_activity`.`query` field. If this value is specified without units, it is taken as bytes. The default value is 1024 bytes. This parameter can only be set at server start. 

`track_counts` (`boolean`)  #
    

Enables collection of statistics on database activity. This parameter is on by default, because the autovacuum daemon needs the collected information. Only superusers and users with the appropriate `SET` privilege can change this setting. 

`track_io_timing` (`boolean`)  #
    

Enables timing of database I/O calls. This parameter is off by default, as it will repeatedly query the operating system for the current time, which may cause significant overhead on some platforms. You can use the [pg_test_timing](pgtesttiming.md "pg_test_timing") tool to measure the overhead of timing on your system. I/O timing information is displayed in [ `pg_stat_database`](monitoring-stats.md#MONITORING-PG-STAT-DATABASE-VIEW "27.2.17. pg_stat_database"), [ `pg_stat_io`](monitoring-stats.md#MONITORING-PG-STAT-IO-VIEW "27.2.13. pg_stat_io"), in the output of [EXPLAIN](sql-explain.md "EXPLAIN") when the `BUFFERS` option is used, in the output of [VACUUM](sql-vacuum.md "VACUUM") when the `VERBOSE` option is used, by autovacuum for auto-vacuums and auto-analyzes, when [log_autovacuum_min_duration](runtime-config-logging.md#GUC-LOG-AUTOVACUUM-MIN-DURATION) is set and by [pg_stat_statements](pgstatstatements.md "F.30. pg_stat_statements — track statistics of SQL planning and execution"). Only superusers and users with the appropriate `SET` privilege can change this setting. 

`track_wal_io_timing` (`boolean`)  #
    

Enables timing of WAL I/O calls. This parameter is off by default, as it will repeatedly query the operating system for the current time, which may cause significant overhead on some platforms. You can use the pg_test_timing tool to measure the overhead of timing on your system. I/O timing information is displayed in [ `pg_stat_wal`](monitoring-stats.md#MONITORING-PG-STAT-WAL-VIEW "27.2.16. pg_stat_wal"). Only superusers and users with the appropriate `SET` privilege can change this setting. 

`track_functions` (`enum`)  #
    

Enables tracking of function call counts and time used. Specify `pl` to track only procedural-language functions, `all` to also track SQL and C language functions. The default is `none`, which disables function statistics tracking. Only superusers and users with the appropriate `SET` privilege can change this setting. 

### Note

SQL-language functions that are simple enough to be “inlined” into the calling query will not be tracked, regardless of this setting. 

`stats_fetch_consistency` (`enum`)  #
    

Determines the behavior when cumulative statistics are accessed multiple times within a transaction. When set to `none`, each access re-fetches counters from shared memory. When set to `cache`, the first access to statistics for an object caches those statistics until the end of the transaction unless `pg_stat_clear_snapshot()` is called. When set to `snapshot`, the first statistics access caches all statistics accessible in the current database, until the end of the transaction unless `pg_stat_clear_snapshot()` is called. Changing this parameter in a transaction discards the statistics snapshot. The default is `cache`. 

### Note

`none` is most suitable for monitoring systems. If values are only accessed once, it is the most efficient. `cache` ensures repeat accesses yield the same values, which is important for queries involving e.g. self-joins. `snapshot` can be useful when interactively inspecting statistics, but has higher overhead, particularly if many database objects exist. 

### 19.9.2. Statistics Monitoring #

`compute_query_id` (`enum`)  #
    

Enables in-core computation of a query identifier. Query identifiers can be displayed in the [`pg_stat_activity`](monitoring-stats.md#MONITORING-PG-STAT-ACTIVITY-VIEW "27.2.3. pg_stat_activity") view, using `EXPLAIN`, or emitted in the log if configured via the [log_line_prefix](runtime-config-logging.md#GUC-LOG-LINE-PREFIX) parameter. The [pg_stat_statements](pgstatstatements.md "F.30. pg_stat_statements — track statistics of SQL planning and execution") extension also requires a query identifier to be computed. Note that an external module can alternatively be used if the in-core query identifier computation method is not acceptable. In this case, in-core computation must be always disabled. Valid values are `off` (always disabled), `on` (always enabled), `auto`, which lets modules such as [pg_stat_statements](pgstatstatements.md "F.30. pg_stat_statements — track statistics of SQL planning and execution") automatically enable it, and `regress` which has the same effect as `auto`, except that the query identifier is not shown in the `EXPLAIN` output in order to facilitate automated regression testing. The default is `auto`. 

### Note

To ensure that only one query identifier is calculated and displayed, extensions that calculate query identifiers should throw an error if a query identifier has already been computed. 

`log_statement_stats` (`boolean`)    
`log_parser_stats` (`boolean`)    
`log_planner_stats` (`boolean`)    
`log_executor_stats` (`boolean`)  #
    

For each query, output performance statistics of the respective module to the server log. This is a crude profiling instrument, similar to the Unix `getrusage()` operating system facility. `log_statement_stats` reports total statement statistics, while the others report per-module statistics. `log_statement_stats` cannot be enabled together with any of the per-module options. All of these options are disabled by default. Only superusers and users with the appropriate `SET` privilege can change these settings. 

* * *

[Prev](runtime-config-logging.md "19.8. Error Reporting and Logging") | [Up](runtime-config.md "Chapter 19. Server Configuration")|  [Next](runtime-config-autovacuum.md "19.10. Automatic Vacuuming")  
---|---|---  
19.8. Error Reporting and Logging | [Home](index.md "PostgreSQL 17.5 Documentation")|  19.10. Automatic Vacuuming

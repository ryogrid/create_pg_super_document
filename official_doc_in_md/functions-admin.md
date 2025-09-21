9.28. System Administration Functions  
---  
[Prev](functions-info.md "9.27. System Information Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-trigger.md "9.29. Trigger Functions")  
  
* * *

## 9.28. System Administration Functions #

[9.28.1. Configuration Settings Functions](functions-admin.md#FUNCTIONS-ADMIN-SET)
[9.28.2. Server Signaling Functions](functions-admin.md#FUNCTIONS-ADMIN-SIGNAL)
[9.28.3. Backup Control Functions](functions-admin.md#FUNCTIONS-ADMIN-BACKUP)
[9.28.4. Recovery Control Functions](functions-admin.md#FUNCTIONS-RECOVERY-CONTROL)
[9.28.5. Snapshot Synchronization Functions](functions-admin.md#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION)
[9.28.6. Replication Management Functions](functions-admin.md#FUNCTIONS-REPLICATION)
[9.28.7. Database Object Management Functions](functions-admin.md#FUNCTIONS-ADMIN-DBOBJECT)
[9.28.8. Index Maintenance Functions](functions-admin.md#FUNCTIONS-ADMIN-INDEX)
[9.28.9. Generic File Access Functions](functions-admin.md#FUNCTIONS-ADMIN-GENFILE)
[9.28.10. Advisory Lock Functions](functions-admin.md#FUNCTIONS-ADVISORY-LOCKS)

The functions described in this section are used to control and monitor a PostgreSQL installation. 

### 9.28.1. Configuration Settings Functions #

[Table 9.93](functions-admin.md#FUNCTIONS-ADMIN-SET-TABLE "Table 9.93. Configuration Settings Functions") shows the functions available to query and alter run-time configuration parameters. 

**Table 9.93. Configuration Settings Functions**

Function  Description  Example(s)   
---  
`current_setting` ( _`setting_name`_ `text` [, _`missing_ok`_ `boolean` ] ) → `text` Returns the current value of the setting _`setting_name`_. If there is no such setting, `current_setting` throws an error unless _`missing_ok`_ is supplied and is `true` (in which case NULL is returned). This function corresponds to the SQL command [SHOW](sql-show.md "SHOW").  `current_setting('datestyle')` → `ISO, MDY`  
`set_config` ( _`setting_name`_ `text`, _`new_value`_ `text`, _`is_local`_ `boolean` ) → `text` Sets the parameter _`setting_name`_ to _`new_value`_ , and returns that value. If _`is_local`_ is `true`, the new value will only apply during the current transaction. If you want the new value to apply for the rest of the current session, use `false` instead. This function corresponds to the SQL command [SET](sql-set.md "SET").  `set_config('log_statement_stats', 'off', false)` → `off`  
  
  


### 9.28.2. Server Signaling Functions #

The functions shown in [Table 9.94](functions-admin.md#FUNCTIONS-ADMIN-SIGNAL-TABLE "Table 9.94. Server Signaling Functions") send control signals to other server processes. Use of these functions is restricted to superusers by default but access may be granted to others using `GRANT`, with noted exceptions. 

Each of these functions returns `true` if the signal was successfully sent and `false` if sending the signal failed. 

**Table 9.94. Server Signaling Functions**

Function  Description   
---  
`pg_cancel_backend` ( _`pid`_ `integer` ) → `boolean` Cancels the current query of the session whose backend process has the specified process ID. This is also allowed if the calling role is a member of the role whose backend is being canceled or the calling role has privileges of `pg_signal_backend`, however only superusers can cancel superuser backends.   
`pg_log_backend_memory_contexts` ( _`pid`_ `integer` ) → `boolean` Requests to log the memory contexts of the backend with the specified process ID. This function can send the request to backends and auxiliary processes except logger. These memory contexts will be logged at `LOG` message level. They will appear in the server log based on the log configuration set (see [Section 19.8](runtime-config-logging.md "19.8. Error Reporting and Logging") for more information), but will not be sent to the client regardless of [client_min_messages](runtime-config-client.md#GUC-CLIENT-MIN-MESSAGES).   
`pg_reload_conf` () → `boolean` Causes all processes of the PostgreSQL server to reload their configuration files. (This is initiated by sending a SIGHUP signal to the postmaster process, which in turn sends SIGHUP to each of its children.) You can use the [`pg_file_settings`](view-pg-file-settings.md "52.7. pg_file_settings"), [`pg_hba_file_rules`](view-pg-hba-file-rules.md "52.9. pg_hba_file_rules") and [`pg_ident_file_mappings`](view-pg-ident-file-mappings.md "52.10. pg_ident_file_mappings") views to check the configuration files for possible errors, before reloading.   
`pg_rotate_logfile` () → `boolean` Signals the log-file manager to switch to a new output file immediately. This works only when the built-in log collector is running, since otherwise there is no log-file manager subprocess.   
`pg_terminate_backend` ( _`pid`_ `integer`, _`timeout`_ `bigint` `DEFAULT` `0` ) → `boolean` Terminates the session whose backend process has the specified process ID. This is also allowed if the calling role is a member of the role whose backend is being terminated or the calling role has privileges of `pg_signal_backend`, however only superusers can terminate superuser backends.  If _`timeout`_ is not specified or zero, this function returns `true` whether the process actually terminates or not, indicating only that the sending of the signal was successful. If the _`timeout`_ is specified (in milliseconds) and greater than zero, the function waits until the process is actually terminated or until the given time has passed. If the process is terminated, the function returns `true`. On timeout, a warning is emitted and `false` is returned.   
  
  


`pg_cancel_backend` and `pg_terminate_backend` send signals (SIGINT or SIGTERM respectively) to backend processes identified by process ID. The process ID of an active backend can be found from the `pid` column of the `pg_stat_activity` view, or by listing the `postgres` processes on the server (using ps on Unix or the Task Manager on Windows). The role of an active backend can be found from the `usename` column of the `pg_stat_activity` view. 

`pg_log_backend_memory_contexts` can be used to log the memory contexts of a backend process. For example: 
    
    
    postgres=# SELECT pg_log_backend_memory_contexts(pg_backend_pid());
     pg_log_backend_memory_contexts
    --------------------------------
     t
    (1 row)
    

One message for each memory context will be logged. For example: 
    
    
    LOG:  logging memory contexts of PID 10377
    STATEMENT:  SELECT pg_log_backend_memory_contexts(pg_backend_pid());
    LOG:  level: 0; TopMemoryContext: 80800 total in 6 blocks; 14432 free (5 chunks); 66368 used
    LOG:  level: 1; pgstat TabStatusArray lookup hash table: 8192 total in 1 blocks; 1408 free (0 chunks); 6784 used
    LOG:  level: 1; TopTransactionContext: 8192 total in 1 blocks; 7720 free (1 chunks); 472 used
    LOG:  level: 1; RowDescriptionContext: 8192 total in 1 blocks; 6880 free (0 chunks); 1312 used
    LOG:  level: 1; MessageContext: 16384 total in 2 blocks; 5152 free (0 chunks); 11232 used
    LOG:  level: 1; Operator class cache: 8192 total in 1 blocks; 512 free (0 chunks); 7680 used
    LOG:  level: 1; smgr relation table: 16384 total in 2 blocks; 4544 free (3 chunks); 11840 used
    LOG:  level: 1; TransactionAbortContext: 32768 total in 1 blocks; 32504 free (0 chunks); 264 used
    ...
    LOG:  level: 1; ErrorContext: 8192 total in 1 blocks; 7928 free (3 chunks); 264 used
    LOG:  Grand total: 1651920 bytes in 201 blocks; 622360 free (88 chunks); 1029560 used
    

If there are more than 100 child contexts under the same parent, the first 100 child contexts are logged, along with a summary of the remaining contexts. Note that frequent calls to this function could incur significant overhead, because it may generate a large number of log messages. 

### 9.28.3. Backup Control Functions #

The functions shown in [Table 9.95](functions-admin.md#FUNCTIONS-ADMIN-BACKUP-TABLE "Table 9.95. Backup Control Functions") assist in making on-line backups. These functions cannot be executed during recovery (except `pg_backup_start`, `pg_backup_stop`, and `pg_wal_lsn_diff`). 

For details about proper usage of these functions, see [Section 25.3](continuous-archiving.md "25.3. Continuous Archiving and Point-in-Time Recovery \(PITR\)"). 

**Table 9.95. Backup Control Functions**

Function  Description   
---  
`pg_create_restore_point` ( _`name`_ `text` ) → `pg_lsn` Creates a named marker record in the write-ahead log that can later be used as a recovery target, and returns the corresponding write-ahead log location. The given name can then be used with [recovery_target_name](runtime-config-wal.md#GUC-RECOVERY-TARGET-NAME) to specify the point up to which recovery will proceed. Avoid creating multiple restore points with the same name, since recovery will stop at the first one whose name matches the recovery target.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_current_wal_flush_lsn` () → `pg_lsn` Returns the current write-ahead log flush location (see notes below).   
`pg_current_wal_insert_lsn` () → `pg_lsn` Returns the current write-ahead log insert location (see notes below).   
`pg_current_wal_lsn` () → `pg_lsn` Returns the current write-ahead log write location (see notes below).   
`pg_backup_start` ( _`label`_ `text` [, _`fast`_ `boolean` ] ) → `pg_lsn` Prepares the server to begin an on-line backup. The only required parameter is an arbitrary user-defined label for the backup. (Typically this would be the name under which the backup dump file will be stored.) If the optional second parameter is given as `true`, it specifies executing `pg_backup_start` as quickly as possible. This forces an immediate checkpoint which will cause a spike in I/O operations, slowing any concurrently executing queries.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_backup_stop` ( [_`wait_for_archive`_ `boolean` ] ) → `record` ( _`lsn`_ `pg_lsn`, _`labelfile`_ `text`, _`spcmapfile`_ `text` )  Finishes performing an on-line backup. The desired contents of the backup label file and the tablespace map file are returned as part of the result of the function and must be written to files in the backup area. These files must not be written to the live data directory (doing so will cause PostgreSQL to fail to restart in the event of a crash).  There is an optional parameter of type `boolean`. If false, the function will return immediately after the backup is completed, without waiting for WAL to be archived. This behavior is only useful with backup software that independently monitors WAL archiving. Otherwise, WAL required to make the backup consistent might be missing and make the backup useless. By default or when this parameter is true, `pg_backup_stop` will wait for WAL to be archived when archiving is enabled. (On a standby, this means that it will wait only when `archive_mode` = `always`. If write activity on the primary is low, it may be useful to run `pg_switch_wal` on the primary in order to trigger an immediate segment switch.)  When executed on a primary, this function also creates a backup history file in the write-ahead log archive area. The history file includes the label given to `pg_backup_start`, the starting and ending write-ahead log locations for the backup, and the starting and ending times of the backup. After recording the ending location, the current write-ahead log insertion point is automatically advanced to the next write-ahead log file, so that the ending write-ahead log file can be archived immediately to complete the backup.  The result of the function is a single record. The _`lsn`_ column holds the backup's ending write-ahead log location (which again can be ignored). The second column returns the contents of the backup label file, and the third column returns the contents of the tablespace map file. These must be stored as part of the backup and are required as part of the restore process.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_switch_wal` () → `pg_lsn` Forces the server to switch to a new write-ahead log file, which allows the current file to be archived (assuming you are using continuous archiving). The result is the ending write-ahead log location plus 1 within the just-completed write-ahead log file. If there has been no write-ahead log activity since the last write-ahead log switch, `pg_switch_wal` does nothing and returns the start location of the write-ahead log file currently in use.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_walfile_name` ( _`lsn`_ `pg_lsn` ) → `text` Converts a write-ahead log location to the name of the WAL file holding that location.   
`pg_walfile_name_offset` ( _`lsn`_ `pg_lsn` ) → `record` ( _`file_name`_ `text`, _`file_offset`_ `integer` )  Converts a write-ahead log location to a WAL file name and byte offset within that file.   
`pg_split_walfile_name` ( _`file_name`_ `text` ) → `record` ( _`segment_number`_ `numeric`, _`timeline_id`_ `bigint` )  Extracts the sequence number and timeline ID from a WAL file name.   
`pg_wal_lsn_diff` ( _`lsn1`_ `pg_lsn`, _`lsn2`_ `pg_lsn` ) → `numeric` Calculates the difference in bytes (_`lsn1`_ \- _`lsn2`_) between two write-ahead log locations. This can be used with `pg_stat_replication` or some of the functions shown in [Table 9.95](functions-admin.md#FUNCTIONS-ADMIN-BACKUP-TABLE "Table 9.95. Backup Control Functions") to get the replication lag.   
  
  


`pg_current_wal_lsn` displays the current write-ahead log write location in the same format used by the above functions. Similarly, `pg_current_wal_insert_lsn` displays the current write-ahead log insertion location and `pg_current_wal_flush_lsn` displays the current write-ahead log flush location. The insertion location is the “logical” end of the write-ahead log at any instant, while the write location is the end of what has actually been written out from the server's internal buffers, and the flush location is the last location known to be written to durable storage. The write location is the end of what can be examined from outside the server, and is usually what you want if you are interested in archiving partially-complete write-ahead log files. The insertion and flush locations are made available primarily for server debugging purposes. These are all read-only operations and do not require superuser permissions. 

You can use `pg_walfile_name_offset` to extract the corresponding write-ahead log file name and byte offset from a `pg_lsn` value. For example: 
    
    
    postgres=# SELECT * FROM pg_walfile_name_offset((pg_backup_stop()).lsn);
            file_name         | file_offset
    --------------------------+-------------
     00000001000000000000000D |     4039624
    (1 row)
    

Similarly, `pg_walfile_name` extracts just the write-ahead log file name. 

`pg_split_walfile_name` is useful to compute a LSN from a file offset and WAL file name, for example: 
    
    
    postgres=# \set file_name '000000010000000100C000AB'
    postgres=# \set offset 256
    postgres=# SELECT '0/0'::pg_lsn + pd.segment_number * ps.setting::int + :offset AS lsn
      FROM pg_split_walfile_name(:'file_name') pd,
           pg_show_all_settings() ps
      WHERE ps.name = 'wal_segment_size';
          lsn
    ---------------
     C001/AB000100
    (1 row)
    

### 9.28.4. Recovery Control Functions #

The functions shown in [Table 9.96](functions-admin.md#FUNCTIONS-RECOVERY-INFO-TABLE "Table 9.96. Recovery Information Functions") provide information about the current status of a standby server. These functions may be executed both during recovery and in normal running. 

**Table 9.96. Recovery Information Functions**

Function  Description   
---  
`pg_is_in_recovery` () → `boolean` Returns true if recovery is still in progress.   
`pg_last_wal_receive_lsn` () → `pg_lsn` Returns the last write-ahead log location that has been received and synced to disk by streaming replication. While streaming replication is in progress this will increase monotonically. If recovery has completed then this will remain static at the location of the last WAL record received and synced to disk during recovery. If streaming replication is disabled, or if it has not yet started, the function returns `NULL`.   
`pg_last_wal_replay_lsn` () → `pg_lsn` Returns the last write-ahead log location that has been replayed during recovery. If recovery is still in progress this will increase monotonically. If recovery has completed then this will remain static at the location of the last WAL record applied during recovery. When the server has been started normally without recovery, the function returns `NULL`.   
`pg_last_xact_replay_timestamp` () → `timestamp with time zone` Returns the time stamp of the last transaction replayed during recovery. This is the time at which the commit or abort WAL record for that transaction was generated on the primary. If no transactions have been replayed during recovery, the function returns `NULL`. Otherwise, if recovery is still in progress this will increase monotonically. If recovery has completed then this will remain static at the time of the last transaction applied during recovery. When the server has been started normally without recovery, the function returns `NULL`.   
`pg_get_wal_resource_managers` () → `setof record` ( _`rm_id`_ `integer`, _`rm_name`_ `text`, _`rm_builtin`_ `boolean` )  Returns the currently-loaded WAL resource managers in the system. The column _`rm_builtin`_ indicates whether it's a built-in resource manager, or a custom resource manager loaded by an extension.   
  
  


The functions shown in [Table 9.97](functions-admin.md#FUNCTIONS-RECOVERY-CONTROL-TABLE "Table 9.97. Recovery Control Functions") control the progress of recovery. These functions may be executed only during recovery. 

**Table 9.97. Recovery Control Functions**

Function  Description   
---  
`pg_is_wal_replay_paused` () → `boolean` Returns true if recovery pause is requested.   
`pg_get_wal_replay_pause_state` () → `text` Returns recovery pause state. The return values are ` not paused` if pause is not requested, ` pause requested` if pause is requested but recovery is not yet paused, and `paused` if the recovery is actually paused.   
`pg_promote` ( _`wait`_ `boolean` `DEFAULT` `true`, _`wait_seconds`_ `integer` `DEFAULT` `60` ) → `boolean` Promotes a standby server to primary status. With _`wait`_ set to `true` (the default), the function waits until promotion is completed or _`wait_seconds`_ seconds have passed, and returns `true` if promotion is successful and `false` otherwise. If _`wait`_ is set to `false`, the function returns `true` immediately after sending a `SIGUSR1` signal to the postmaster to trigger promotion.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_wal_replay_pause` () → `void` Request to pause recovery. A request doesn't mean that recovery stops right away. If you want a guarantee that recovery is actually paused, you need to check for the recovery pause state returned by `pg_get_wal_replay_pause_state()`. Note that `pg_is_wal_replay_paused()` returns whether a request is made. While recovery is paused, no further database changes are applied. If hot standby is active, all new queries will see the same consistent snapshot of the database, and no further query conflicts will be generated until recovery is resumed.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_wal_replay_resume` () → `void` Restarts recovery if it was paused.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
  
  


`pg_wal_replay_pause` and `pg_wal_replay_resume` cannot be executed while a promotion is ongoing. If a promotion is triggered while recovery is paused, the paused state ends and promotion continues. 

If streaming replication is disabled, the paused state may continue indefinitely without a problem. If streaming replication is in progress then WAL records will continue to be received, which will eventually fill available disk space, depending upon the duration of the pause, the rate of WAL generation and available disk space. 

### 9.28.5. Snapshot Synchronization Functions #

PostgreSQL allows database sessions to synchronize their snapshots. A _snapshot_ determines which data is visible to the transaction that is using the snapshot. Synchronized snapshots are necessary when two or more sessions need to see identical content in the database. If two sessions just start their transactions independently, there is always a possibility that some third transaction commits between the executions of the two `START TRANSACTION` commands, so that one session sees the effects of that transaction and the other does not. 

To solve this problem, PostgreSQL allows a transaction to _export_ the snapshot it is using. As long as the exporting transaction remains open, other transactions can _import_ its snapshot, and thereby be guaranteed that they see exactly the same view of the database that the first transaction sees. But note that any database changes made by any one of these transactions remain invisible to the other transactions, as is usual for changes made by uncommitted transactions. So the transactions are synchronized with respect to pre-existing data, but act normally for changes they make themselves. 

Snapshots are exported with the `pg_export_snapshot` function, shown in [Table 9.98](functions-admin.md#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION-TABLE "Table 9.98. Snapshot Synchronization Functions"), and imported with the [SET TRANSACTION](sql-set-transaction.md "SET TRANSACTION") command. 

**Table 9.98. Snapshot Synchronization Functions**

Function  Description   
---  
`pg_export_snapshot` () → `text` Saves the transaction's current snapshot and returns a `text` string identifying the snapshot. This string must be passed (outside the database) to clients that want to import the snapshot. The snapshot is available for import only until the end of the transaction that exported it.  A transaction can export more than one snapshot, if needed. Note that doing so is only useful in `READ COMMITTED` transactions, since in `REPEATABLE READ` and higher isolation levels, transactions use the same snapshot throughout their lifetime. Once a transaction has exported any snapshots, it cannot be prepared with [PREPARE TRANSACTION](sql-prepare-transaction.md "PREPARE TRANSACTION").   
`pg_log_standby_snapshot` () → `pg_lsn` Take a snapshot of running transactions and write it to WAL, without having to wait for bgwriter or checkpointer to log one. This is useful for logical decoding on standby, as logical slot creation has to wait until such a record is replayed on the standby.   
  
  


### 9.28.6. Replication Management Functions #

The functions shown in [Table 9.99](functions-admin.md#FUNCTIONS-REPLICATION-TABLE "Table 9.99. Replication Management Functions") are for controlling and interacting with replication features. See [Section 26.2.5](warm-standby.md#STREAMING-REPLICATION "26.2.5. Streaming Replication"), [Section 26.2.6](warm-standby.md#STREAMING-REPLICATION-SLOTS "26.2.6. Replication Slots"), and [Chapter 48](replication-origins.md "Chapter 48. Replication Progress Tracking") for information about the underlying features. Use of functions for replication origin is only allowed to the superuser by default, but may be allowed to other users by using the `GRANT` command. Use of functions for replication slots is restricted to superusers and users having `REPLICATION` privilege. 

Many of these functions have equivalent commands in the replication protocol; see [Section 53.4](protocol-replication.md "53.4. Streaming Replication Protocol"). 

The functions described in [Section 9.28.3](functions-admin.md#FUNCTIONS-ADMIN-BACKUP "9.28.3. Backup Control Functions"), [Section 9.28.4](functions-admin.md#FUNCTIONS-RECOVERY-CONTROL "9.28.4. Recovery Control Functions"), and [Section 9.28.5](functions-admin.md#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION "9.28.5. Snapshot Synchronization Functions") are also relevant for replication. 

**Table 9.99. Replication Management Functions**

Function  Description   
---  
`pg_create_physical_replication_slot` ( _`slot_name`_ `name` [, _`immediately_reserve`_ `boolean`, _`temporary`_ `boolean` ] ) → `record` ( _`slot_name`_ `name`, _`lsn`_ `pg_lsn` )  Creates a new physical replication slot named _`slot_name`_. The optional second parameter, when `true`, specifies that the LSN for this replication slot be reserved immediately; otherwise the LSN is reserved on first connection from a streaming replication client. Streaming changes from a physical slot is only possible with the streaming-replication protocol — see [Section 53.4](protocol-replication.md "53.4. Streaming Replication Protocol"). The optional third parameter, _`temporary`_ , when set to true, specifies that the slot should not be permanently stored to disk and is only meant for use by the current session. Temporary slots are also released upon any error. This function corresponds to the replication protocol command `CREATE_REPLICATION_SLOT ... PHYSICAL`.   
`pg_drop_replication_slot` ( _`slot_name`_ `name` ) → `void` Drops the physical or logical replication slot named _`slot_name`_. Same as replication protocol command `DROP_REPLICATION_SLOT`.   
`pg_create_logical_replication_slot` ( _`slot_name`_ `name`, _`plugin`_ `name` [, _`temporary`_ `boolean`, _`twophase`_ `boolean`, _`failover`_ `boolean` ] ) → `record` ( _`slot_name`_ `name`, _`lsn`_ `pg_lsn` )  Creates a new logical (decoding) replication slot named _`slot_name`_ using the output plugin _`plugin`_. The optional third parameter, _`temporary`_ , when set to true, specifies that the slot should not be permanently stored to disk and is only meant for use by the current session. Temporary slots are also released upon any error. The optional fourth parameter, _`twophase`_ , when set to true, specifies that the decoding of prepared transactions is enabled for this slot. The optional fifth parameter, _`failover`_ , when set to true, specifies that this slot is enabled to be synced to the standbys so that logical replication can be resumed after failover. A call to this function has the same effect as the replication protocol command `CREATE_REPLICATION_SLOT ... LOGICAL`.   
`pg_copy_physical_replication_slot` ( _`src_slot_name`_ `name`, _`dst_slot_name`_ `name` [, _`temporary`_ `boolean` ] ) → `record` ( _`slot_name`_ `name`, _`lsn`_ `pg_lsn` )  Copies an existing physical replication slot named _`src_slot_name`_ to a physical replication slot named _`dst_slot_name`_. The copied physical slot starts to reserve WAL from the same LSN as the source slot. _`temporary`_ is optional. If _`temporary`_ is omitted, the same value as the source slot is used. Copy of an invalidated slot is not allowed.   
`pg_copy_logical_replication_slot` ( _`src_slot_name`_ `name`, _`dst_slot_name`_ `name` [, _`temporary`_ `boolean` [, _`plugin`_ `name` ]] ) → `record` ( _`slot_name`_ `name`, _`lsn`_ `pg_lsn` )  Copies an existing logical replication slot named _`src_slot_name`_ to a logical replication slot named _`dst_slot_name`_ , optionally changing the output plugin and persistence. The copied logical slot starts from the same LSN as the source logical slot. Both _`temporary`_ and _`plugin`_ are optional; if they are omitted, the values of the source slot are used. The `failover` option of the source logical slot is not copied and is set to `false` by default. This is to avoid the risk of being unable to continue logical replication after failover to standby where the slot is being synchronized. Copy of an invalidated slot is not allowed.   
`pg_logical_slot_get_changes` ( _`slot_name`_ `name`, _`upto_lsn`_ `pg_lsn`, _`upto_nchanges`_ `integer`, `VARIADIC` _`options`_ `text[]` ) → `setof record` ( _`lsn`_ `pg_lsn`, _`xid`_ `xid`, _`data`_ `text` )  Returns changes in the slot _`slot_name`_ , starting from the point from which changes have been consumed last. If _`upto_lsn`_ and _`upto_nchanges`_ are NULL, logical decoding will continue until end of WAL. If _`upto_lsn`_ is non-NULL, decoding will include only those transactions which commit prior to the specified LSN. If _`upto_nchanges`_ is non-NULL, decoding will stop when the number of rows produced by decoding exceeds the specified value. Note, however, that the actual number of rows returned may be larger, since this limit is only checked after adding the rows produced when decoding each new transaction commit. If the specified slot is a logical failover slot then the function will not return until all physical slots specified in [`synchronized_standby_slots`](runtime-config-replication.md#GUC-SYNCHRONIZED-STANDBY-SLOTS) have confirmed WAL receipt.   
`pg_logical_slot_peek_changes` ( _`slot_name`_ `name`, _`upto_lsn`_ `pg_lsn`, _`upto_nchanges`_ `integer`, `VARIADIC` _`options`_ `text[]` ) → `setof record` ( _`lsn`_ `pg_lsn`, _`xid`_ `xid`, _`data`_ `text` )  Behaves just like the `pg_logical_slot_get_changes()` function, except that changes are not consumed; that is, they will be returned again on future calls.   
`pg_logical_slot_get_binary_changes` ( _`slot_name`_ `name`, _`upto_lsn`_ `pg_lsn`, _`upto_nchanges`_ `integer`, `VARIADIC` _`options`_ `text[]` ) → `setof record` ( _`lsn`_ `pg_lsn`, _`xid`_ `xid`, _`data`_ `bytea` )  Behaves just like the `pg_logical_slot_get_changes()` function, except that changes are returned as `bytea`.   
`pg_logical_slot_peek_binary_changes` ( _`slot_name`_ `name`, _`upto_lsn`_ `pg_lsn`, _`upto_nchanges`_ `integer`, `VARIADIC` _`options`_ `text[]` ) → `setof record` ( _`lsn`_ `pg_lsn`, _`xid`_ `xid`, _`data`_ `bytea` )  Behaves just like the `pg_logical_slot_peek_changes()` function, except that changes are returned as `bytea`.   
`pg_replication_slot_advance` ( _`slot_name`_ `name`, _`upto_lsn`_ `pg_lsn` ) → `record` ( _`slot_name`_ `name`, _`end_lsn`_ `pg_lsn` )  Advances the current confirmed position of a replication slot named _`slot_name`_. The slot will not be moved backwards, and it will not be moved beyond the current insert location. Returns the name of the slot and the actual position that it was advanced to. The updated slot position information is written out at the next checkpoint if any advancing is done. So in the event of a crash, the slot may return to an earlier position. If the specified slot is a logical failover slot then the function will not return until all physical slots specified in [`synchronized_standby_slots`](runtime-config-replication.md#GUC-SYNCHRONIZED-STANDBY-SLOTS) have confirmed WAL receipt.   
`pg_replication_origin_create` ( _`node_name`_ `text` ) → `oid` Creates a replication origin with the given external name, and returns the internal ID assigned to it.   
`pg_replication_origin_drop` ( _`node_name`_ `text` ) → `void` Deletes a previously-created replication origin, including any associated replay progress.   
`pg_replication_origin_oid` ( _`node_name`_ `text` ) → `oid` Looks up a replication origin by name and returns the internal ID. If no such replication origin is found, `NULL` is returned.   
`pg_replication_origin_session_setup` ( _`node_name`_ `text` ) → `void` Marks the current session as replaying from the given origin, allowing replay progress to be tracked. Can only be used if no origin is currently selected. Use `pg_replication_origin_session_reset` to undo.   
`pg_replication_origin_session_reset` () → `void` Cancels the effects of `pg_replication_origin_session_setup()`.   
`pg_replication_origin_session_is_setup` () → `boolean` Returns true if a replication origin has been selected in the current session.   
`pg_replication_origin_session_progress` ( _`flush`_ `boolean` ) → `pg_lsn` Returns the replay location for the replication origin selected in the current session. The parameter _`flush`_ determines whether the corresponding local transaction will be guaranteed to have been flushed to disk or not.   
`pg_replication_origin_xact_setup` ( _`origin_lsn`_ `pg_lsn`, _`origin_timestamp`_ `timestamp with time zone` ) → `void` Marks the current transaction as replaying a transaction that has committed at the given LSN and timestamp. Can only be called when a replication origin has been selected using `pg_replication_origin_session_setup`.   
`pg_replication_origin_xact_reset` () → `void` Cancels the effects of `pg_replication_origin_xact_setup()`.   
`pg_replication_origin_advance` ( _`node_name`_ `text`, _`lsn`_ `pg_lsn` ) → `void` Sets replication progress for the given node to the given location. This is primarily useful for setting up the initial location, or setting a new location after configuration changes and similar. Be aware that careless use of this function can lead to inconsistently replicated data.   
`pg_replication_origin_progress` ( _`node_name`_ `text`, _`flush`_ `boolean` ) → `pg_lsn` Returns the replay location for the given replication origin. The parameter _`flush`_ determines whether the corresponding local transaction will be guaranteed to have been flushed to disk or not.   
`pg_logical_emit_message` ( _`transactional`_ `boolean`, _`prefix`_ `text`, _`content`_ `text` [, _`flush`_ `boolean` `DEFAULT` `false`] ) → `pg_lsn` `pg_logical_emit_message` ( _`transactional`_ `boolean`, _`prefix`_ `text`, _`content`_ `bytea` [, _`flush`_ `boolean` `DEFAULT` `false`] ) → `pg_lsn` Emits a logical decoding message. This can be used to pass generic messages to logical decoding plugins through WAL. The _`transactional`_ parameter specifies if the message should be part of the current transaction, or if it should be written immediately and decoded as soon as the logical decoder reads the record. The _`prefix`_ parameter is a textual prefix that can be used by logical decoding plugins to easily recognize messages that are interesting for them. The _`content`_ parameter is the content of the message, given either in text or binary form. The _`flush`_ parameter (default set to `false`) controls if the message is immediately flushed to WAL or not. _`flush`_ has no effect with _`transactional`_ , as the message's WAL record is flushed along with its transaction.   
`pg_sync_replication_slots` () → `void` Synchronize the logical failover replication slots from the primary server to the standby server. This function can only be executed on the standby server. Temporary synced slots, if any, cannot be used for logical decoding and must be dropped after promotion. See [Section 47.2.3](logicaldecoding-explanation.md#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION "47.2.3. Replication Slot Synchronization") for details. Note that this function is primarily intended for testing and debugging purposes and should be used with caution. Additionaly, this function cannot be executed if [` sync_replication_slots`](runtime-config-replication.md#GUC-SYNC-REPLICATION-SLOTS) is enabled and the slotsync worker is already running to perform the synchronization of slots. 

### Caution

If, after executing the function, [ `hot_standby_feedback`](runtime-config-replication.md#GUC-HOT-STANDBY-FEEDBACK) is disabled on the standby or the physical slot configured in [ `primary_slot_name`](runtime-config-replication.md#GUC-PRIMARY-SLOT-NAME) is removed, then it is possible that the necessary rows of the synchronized slot will be removed by the VACUUM process on the primary server, resulting in the synchronized slot becoming invalidated.   
  
  


### 9.28.7. Database Object Management Functions #

The functions shown in [Table 9.100](functions-admin.md#FUNCTIONS-ADMIN-DBSIZE "Table 9.100. Database Object Size Functions") calculate the disk space usage of database objects, or assist in presentation or understanding of usage results. `bigint` results are measured in bytes. If an OID that does not represent an existing object is passed to one of these functions, `NULL` is returned. 

**Table 9.100. Database Object Size Functions**

Function  Description   
---  
`pg_column_size` ( `"any"` ) → `integer` Shows the number of bytes used to store any individual data value. If applied directly to a table column value, this reflects any compression that was done.   
`pg_column_compression` ( `"any"` ) → `text` Shows the compression algorithm that was used to compress an individual variable-length value. Returns `NULL` if the value is not compressed.   
`pg_column_toast_chunk_id` ( `"any"` ) → `oid` Shows the `chunk_id` of an on-disk TOASTed value. Returns `NULL` if the value is un-TOASTed or not on-disk. See [Section 65.2](storage-toast.md "65.2. TOAST") for more information about TOAST.   
`pg_database_size` ( `name` ) → `bigint` `pg_database_size` ( `oid` ) → `bigint` Computes the total disk space used by the database with the specified name or OID. To use this function, you must have `CONNECT` privilege on the specified database (which is granted by default) or have privileges of the `pg_read_all_stats` role.   
`pg_indexes_size` ( `regclass` ) → `bigint` Computes the total disk space used by indexes attached to the specified table.   
`pg_relation_size` ( _`relation`_ `regclass` [, _`fork`_ `text` ] ) → `bigint` Computes the disk space used by one “fork” of the specified relation. (Note that for most purposes it is more convenient to use the higher-level functions `pg_total_relation_size` or `pg_table_size`, which sum the sizes of all forks.) With one argument, this returns the size of the main data fork of the relation. The second argument can be provided to specify which fork to examine: 

  * `main` returns the size of the main data fork of the relation. 
  * `fsm` returns the size of the Free Space Map (see [Section 65.3](storage-fsm.md "65.3. Free Space Map")) associated with the relation. 
  * `vm` returns the size of the Visibility Map (see [Section 65.4](storage-vm.md "65.4. Visibility Map")) associated with the relation. 
  * `init` returns the size of the initialization fork, if any, associated with the relation. 

  
`pg_size_bytes` ( `text` ) → `bigint` Converts a size in human-readable format (as returned by `pg_size_pretty`) into bytes. Valid units are `bytes`, `B`, `kB`, `MB`, `GB`, `TB`, and `PB`.   
`pg_size_pretty` ( `bigint` ) → `text` `pg_size_pretty` ( `numeric` ) → `text` Converts a size in bytes into a more easily human-readable format with size units (bytes, kB, MB, GB, TB, or PB as appropriate). Note that the units are powers of 2 rather than powers of 10, so 1kB is 1024 bytes, 1MB is 10242 = 1048576 bytes, and so on.   
`pg_table_size` ( `regclass` ) → `bigint` Computes the disk space used by the specified table, excluding indexes (but including its TOAST table if any, free space map, and visibility map).   
`pg_tablespace_size` ( `name` ) → `bigint` `pg_tablespace_size` ( `oid` ) → `bigint` Computes the total disk space used in the tablespace with the specified name or OID. To use this function, you must have `CREATE` privilege on the specified tablespace or have privileges of the `pg_read_all_stats` role, unless it is the default tablespace for the current database.   
`pg_total_relation_size` ( `regclass` ) → `bigint` Computes the total disk space used by the specified table, including all indexes and TOAST data. The result is equivalent to `pg_table_size` `+` `pg_indexes_size`.   
  
  


The functions above that operate on tables or indexes accept a `regclass` argument, which is simply the OID of the table or index in the `pg_class` system catalog. You do not have to look up the OID by hand, however, since the `regclass` data type's input converter will do the work for you. See [Section 8.19](datatype-oid.md "8.19. Object Identifier Types") for details. 

The functions shown in [Table 9.101](functions-admin.md#FUNCTIONS-ADMIN-DBLOCATION "Table 9.101. Database Object Location Functions") assist in identifying the specific disk files associated with database objects. 

**Table 9.101. Database Object Location Functions**

Function  Description   
---  
`pg_relation_filenode` ( _`relation`_ `regclass` ) → `oid` Returns the “filenode” number currently assigned to the specified relation. The filenode is the base component of the file name(s) used for the relation (see [Section 65.1](storage-file-layout.md "65.1. Database File Layout") for more information). For most relations the result is the same as `pg_class`.`relfilenode`, but for certain system catalogs `relfilenode` is zero and this function must be used to get the correct value. The function returns NULL if passed a relation that does not have storage, such as a view.   
`pg_relation_filepath` ( _`relation`_ `regclass` ) → `text` Returns the entire file path name (relative to the database cluster's data directory, `PGDATA`) of the relation.   
`pg_filenode_relation` ( _`tablespace`_ `oid`, _`filenode`_ `oid` ) → `regclass` Returns a relation's OID given the tablespace OID and filenode it is stored under. This is essentially the inverse mapping of `pg_relation_filepath`. For a relation in the database's default tablespace, the tablespace can be specified as zero. Returns `NULL` if no relation in the current database is associated with the given values.   
  
  


[Table 9.102](functions-admin.md#FUNCTIONS-ADMIN-COLLATION "Table 9.102. Collation Management Functions") lists functions used to manage collations. 

**Table 9.102. Collation Management Functions**

Function  Description   
---  
`pg_collation_actual_version` ( `oid` ) → `text` Returns the actual version of the collation object as it is currently installed in the operating system. If this is different from the value in `pg_collation`.`collversion`, then objects depending on the collation might need to be rebuilt. See also [ALTER COLLATION](sql-altercollation.md "ALTER COLLATION").   
`pg_database_collation_actual_version` ( `oid` ) → `text` Returns the actual version of the database's collation as it is currently installed in the operating system. If this is different from the value in `pg_database`.`datcollversion`, then objects depending on the collation might need to be rebuilt. See also [ALTER DATABASE](sql-alterdatabase.md "ALTER DATABASE").   
`pg_import_system_collations` ( _`schema`_ `regnamespace` ) → `integer` Adds collations to the system catalog `pg_collation` based on all the locales it finds in the operating system. This is what `initdb` uses; see [Section 23.2.2](collation.md#COLLATION-MANAGING "23.2.2. Managing Collations") for more details. If additional locales are installed into the operating system later on, this function can be run again to add collations for the new locales. Locales that match existing entries in `pg_collation` will be skipped. (But collation objects based on locales that are no longer present in the operating system are not removed by this function.) The _`schema`_ parameter would typically be `pg_catalog`, but that is not a requirement; the collations could be installed into some other schema as well. The function returns the number of new collation objects it created. Use of this function is restricted to superusers.   
  
  


[Table 9.103](functions-admin.md#FUNCTIONS-INFO-PARTITION "Table 9.103. Partitioning Information Functions") lists functions that provide information about the structure of partitioned tables. 

**Table 9.103. Partitioning Information Functions**

Function  Description   
---  
`pg_partition_tree` ( `regclass` ) → `setof record` ( _`relid`_ `regclass`, _`parentrelid`_ `regclass`, _`isleaf`_ `boolean`, _`level`_ `integer` )  Lists the tables or indexes in the partition tree of the given partitioned table or partitioned index, with one row for each partition. Information provided includes the OID of the partition, the OID of its immediate parent, a boolean value telling if the partition is a leaf, and an integer telling its level in the hierarchy. The level value is 0 for the input table or index, 1 for its immediate child partitions, 2 for their partitions, and so on. Returns no rows if the relation does not exist or is not a partition or partitioned table.   
`pg_partition_ancestors` ( `regclass` ) → `setof regclass` Lists the ancestor relations of the given partition, including the relation itself. Returns no rows if the relation does not exist or is not a partition or partitioned table.   
`pg_partition_root` ( `regclass` ) → `regclass` Returns the top-most parent of the partition tree to which the given relation belongs. Returns `NULL` if the relation does not exist or is not a partition or partitioned table.   
  
  


For example, to check the total size of the data contained in a partitioned table `measurement`, one could use the following query: 
    
    
    SELECT pg_size_pretty(sum(pg_relation_size(relid))) AS total_size
      FROM pg_partition_tree('measurement');
    

### 9.28.8. Index Maintenance Functions #

[Table 9.104](functions-admin.md#FUNCTIONS-ADMIN-INDEX-TABLE "Table 9.104. Index Maintenance Functions") shows the functions available for index maintenance tasks. (Note that these maintenance tasks are normally done automatically by autovacuum; use of these functions is only required in special cases.) These functions cannot be executed during recovery. Use of these functions is restricted to superusers and the owner of the given index. 

**Table 9.104. Index Maintenance Functions**

Function  Description   
---  
`brin_summarize_new_values` ( _`index`_ `regclass` ) → `integer` Scans the specified BRIN index to find page ranges in the base table that are not currently summarized by the index; for any such range it creates a new summary index tuple by scanning those table pages. Returns the number of new page range summaries that were inserted into the index.   
`brin_summarize_range` ( _`index`_ `regclass`, _`blockNumber`_ `bigint` ) → `integer` Summarizes the page range covering the given block, if not already summarized. This is like `brin_summarize_new_values` except that it only processes the page range that covers the given table block number.   
`brin_desummarize_range` ( _`index`_ `regclass`, _`blockNumber`_ `bigint` ) → `void` Removes the BRIN index tuple that summarizes the page range covering the given table block, if there is one.   
`gin_clean_pending_list` ( _`index`_ `regclass` ) → `bigint` Cleans up the “pending” list of the specified GIN index by moving entries in it, in bulk, to the main GIN data structure. Returns the number of pages removed from the pending list. If the argument is a GIN index built with the `fastupdate` option disabled, no cleanup happens and the result is zero, because the index doesn't have a pending list. See [Section 64.4.4.1](gin.md#GIN-FAST-UPDATE "64.4.4.1. GIN Fast Update Technique") and [Section 64.4.5](gin.md#GIN-TIPS "64.4.5. GIN Tips and Tricks") for details about the pending list and `fastupdate` option.   
  
  


### 9.28.9. Generic File Access Functions #

The functions shown in [Table 9.105](functions-admin.md#FUNCTIONS-ADMIN-GENFILE-TABLE "Table 9.105. Generic File Access Functions") provide native access to files on the machine hosting the server. Only files within the database cluster directory and the `log_directory` can be accessed, unless the user is a superuser or is granted the role `pg_read_server_files`. Use a relative path for files in the cluster directory, and a path matching the `log_directory` configuration setting for log files. 

Note that granting users the EXECUTE privilege on `pg_read_file()`, or related functions, allows them the ability to read any file on the server that the database server process can read; these functions bypass all in-database privilege checks. This means that, for example, a user with such access is able to read the contents of the `pg_authid` table where authentication information is stored, as well as read any table data in the database. Therefore, granting access to these functions should be carefully considered. 

When granting privilege on these functions, note that the table entries showing optional parameters are mostly implemented as several physical functions with different parameter lists. Privilege must be granted separately on each such function, if it is to be used. psql's `\df` command can be useful to check what the actual function signatures are. 

Some of these functions take an optional _`missing_ok`_ parameter, which specifies the behavior when the file or directory does not exist. If `true`, the function returns `NULL` or an empty result set, as appropriate. If `false`, an error is raised. (Failure conditions other than “file not found” are reported as errors in any case.) The default is `false`. 

**Table 9.105. Generic File Access Functions**

Function  Description   
---  
`pg_ls_dir` ( _`dirname`_ `text` [, _`missing_ok`_ `boolean`, _`include_dot_dirs`_ `boolean` ] ) → `setof text` Returns the names of all files (and directories and other special files) in the specified directory. The _`include_dot_dirs`_ parameter indicates whether “.” and “..” are to be included in the result set; the default is to exclude them. Including them can be useful when _`missing_ok`_ is `true`, to distinguish an empty directory from a non-existent directory.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_logdir` () → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the server's log directory. Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and roles with privileges of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_waldir` () → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the server's write-ahead log (WAL) directory. Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and roles with privileges of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_logicalmapdir` () → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the server's `pg_logical/mappings` directory. Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and members of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_logicalsnapdir` () → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the server's `pg_logical/snapshots` directory. Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and members of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_replslotdir` ( _`slot_name`_ `text` ) → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the server's `pg_replslot/slot_name` directory, where _`slot_name`_ is the name of the replication slot provided as input of the function. Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and members of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_archive_statusdir` () → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the server's WAL archive status directory (`pg_wal/archive_status`). Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and members of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_ls_tmpdir` ( [ _`tablespace`_ `oid` ] ) → `setof record` ( _`name`_ `text`, _`size`_ `bigint`, _`modification`_ `timestamp with time zone` )  Returns the name, size, and last modification time (mtime) of each ordinary file in the temporary file directory for the specified _`tablespace`_. If _`tablespace`_ is not provided, the `pg_default` tablespace is examined. Filenames beginning with a dot, directories, and other special files are excluded.  This function is restricted to superusers and members of the `pg_monitor` role by default, but other users can be granted EXECUTE to run the function.   
`pg_read_file` ( _`filename`_ `text` [, _`offset`_ `bigint`, _`length`_ `bigint` ] [, _`missing_ok`_ `boolean` ] ) → `text` Returns all or part of a text file, starting at the given byte _`offset`_ , returning at most _`length`_ bytes (less if the end of file is reached first). If _`offset`_ is negative, it is relative to the end of the file. If _`offset`_ and _`length`_ are omitted, the entire file is returned. The bytes read from the file are interpreted as a string in the database's encoding; an error is thrown if they are not valid in that encoding.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
`pg_read_binary_file` ( _`filename`_ `text` [, _`offset`_ `bigint`, _`length`_ `bigint` ] [, _`missing_ok`_ `boolean` ] ) → `bytea` Returns all or part of a file. This function is identical to `pg_read_file` except that it can read arbitrary binary data, returning the result as `bytea` not `text`; accordingly, no encoding checks are performed.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.  In combination with the `convert_from` function, this function can be used to read a text file in a specified encoding and convert to the database's encoding: 
    
    
    SELECT convert_from(pg_read_binary_file('file_in_utf8.txt'), 'UTF8');
      
  
`pg_stat_file` ( _`filename`_ `text` [, _`missing_ok`_ `boolean` ] ) → `record` ( _`size`_ `bigint`, _`access`_ `timestamp with time zone`, _`modification`_ `timestamp with time zone`, _`change`_ `timestamp with time zone`, _`creation`_ `timestamp with time zone`, _`isdir`_ `boolean` )  Returns a record containing the file's size, last access time stamp, last modification time stamp, last file status change time stamp (Unix platforms only), file creation time stamp (Windows only), and a flag indicating if it is a directory.  This function is restricted to superusers by default, but other users can be granted EXECUTE to run the function.   
  
  


### 9.28.10. Advisory Lock Functions #

The functions shown in [Table 9.106](functions-admin.md#FUNCTIONS-ADVISORY-LOCKS-TABLE "Table 9.106. Advisory Lock Functions") manage advisory locks. For details about proper use of these functions, see [Section 13.3.5](explicit-locking.md#ADVISORY-LOCKS "13.3.5. Advisory Locks"). 

All these functions are intended to be used to lock application-defined resources, which can be identified either by a single 64-bit key value or two 32-bit key values (note that these two key spaces do not overlap). If another session already holds a conflicting lock on the same resource identifier, the functions will either wait until the resource becomes available, or return a `false` result, as appropriate for the function. Locks can be either shared or exclusive: a shared lock does not conflict with other shared locks on the same resource, only with exclusive locks. Locks can be taken at session level (so that they are held until released or the session ends) or at transaction level (so that they are held until the current transaction ends; there is no provision for manual release). Multiple session-level lock requests stack, so that if the same resource identifier is locked three times there must then be three unlock requests to release the resource in advance of session end. 

**Table 9.106. Advisory Lock Functions**

Function  Description   
---  
`pg_advisory_lock` ( _`key`_ `bigint` ) → `void` `pg_advisory_lock` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `void` Obtains an exclusive session-level advisory lock, waiting if necessary.   
`pg_advisory_lock_shared` ( _`key`_ `bigint` ) → `void` `pg_advisory_lock_shared` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `void` Obtains a shared session-level advisory lock, waiting if necessary.   
`pg_advisory_unlock` ( _`key`_ `bigint` ) → `boolean` `pg_advisory_unlock` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `boolean` Releases a previously-acquired exclusive session-level advisory lock. Returns `true` if the lock is successfully released. If the lock was not held, `false` is returned, and in addition, an SQL warning will be reported by the server.   
`pg_advisory_unlock_all` () → `void` Releases all session-level advisory locks held by the current session. (This function is implicitly invoked at session end, even if the client disconnects ungracefully.)   
`pg_advisory_unlock_shared` ( _`key`_ `bigint` ) → `boolean` `pg_advisory_unlock_shared` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `boolean` Releases a previously-acquired shared session-level advisory lock. Returns `true` if the lock is successfully released. If the lock was not held, `false` is returned, and in addition, an SQL warning will be reported by the server.   
`pg_advisory_xact_lock` ( _`key`_ `bigint` ) → `void` `pg_advisory_xact_lock` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `void` Obtains an exclusive transaction-level advisory lock, waiting if necessary.   
`pg_advisory_xact_lock_shared` ( _`key`_ `bigint` ) → `void` `pg_advisory_xact_lock_shared` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `void` Obtains a shared transaction-level advisory lock, waiting if necessary.   
`pg_try_advisory_lock` ( _`key`_ `bigint` ) → `boolean` `pg_try_advisory_lock` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `boolean` Obtains an exclusive session-level advisory lock if available. This will either obtain the lock immediately and return `true`, or return `false` without waiting if the lock cannot be acquired immediately.   
`pg_try_advisory_lock_shared` ( _`key`_ `bigint` ) → `boolean` `pg_try_advisory_lock_shared` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `boolean` Obtains a shared session-level advisory lock if available. This will either obtain the lock immediately and return `true`, or return `false` without waiting if the lock cannot be acquired immediately.   
`pg_try_advisory_xact_lock` ( _`key`_ `bigint` ) → `boolean` `pg_try_advisory_xact_lock` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `boolean` Obtains an exclusive transaction-level advisory lock if available. This will either obtain the lock immediately and return `true`, or return `false` without waiting if the lock cannot be acquired immediately.   
`pg_try_advisory_xact_lock_shared` ( _`key`_ `bigint` ) → `boolean` `pg_try_advisory_xact_lock_shared` ( _`key1`_ `integer`, _`key2`_ `integer` ) → `boolean` Obtains a shared transaction-level advisory lock if available. This will either obtain the lock immediately and return `true`, or return `false` without waiting if the lock cannot be acquired immediately.   
  
  


* * *

[Prev](functions-info.md "9.27. System Information Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-trigger.md "9.29. Trigger Functions")  
---|---|---  
9.27. System Information Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.29. Trigger Functions

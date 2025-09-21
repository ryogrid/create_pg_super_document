pg_createsubscriber  
---  
[Prev](app-pgcontroldata.md "pg_controldata") | [Up](reference-server.md "PostgreSQL Server Applications")| PostgreSQL Server Applications| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](app-pg-ctl.md "pg_ctl")  
  
* * *

## pg_createsubscriber

pg_createsubscriber — convert a physical replica into a new logical replica

## Synopsis

`pg_createsubscriber` [_`option`_...] { `-d` | `--database` }_`dbname`_ { `-D` | `--pgdata` }_`datadir`_ { `-P` | `--publisher-server` }_`connstr`_

## Description

pg_createsubscriber creates a new logical replica from a physical standby server. All tables in the specified database are included in the [logical replication](logical-replication.md "Chapter 29. Logical Replication") setup. A pair of publication and subscription objects are created for each database. It must be run at the target server. 

After a successful run, the state of the target server is analogous to a fresh logical replication setup. The main difference between the logical replication setup and pg_createsubscriber is how the data synchronization is done. pg_createsubscriber does not copy the initial table data. It does only the synchronization phase, which ensures each table is brought up to a synchronized state. 

pg_createsubscriber targets large database systems because in logical replication setup, most of the time is spent doing the initial data copy. Furthermore, a side effect of this long time spent synchronizing data is usually a large amount of changes to be applied (that were produced during the initial data copy), which increases even more the time when the logical replica will be available. For smaller databases, it is recommended to set up logical replication with initial data synchronization. For details, see the `CREATE SUBSCRIPTION` [ `copy_data`](sql-createsubscription.md#SQL-CREATESUBSCRIPTION-PARAMS-WITH-COPY-DATA) option. 

## Options

pg_createsubscriber accepts the following command-line arguments: 

`-d _`dbname`_`  
`--database=_`dbname`_`
    

The name of the database in which to create a subscription. Multiple databases can be selected by writing multiple `-d` switches. 

`-D _`directory`_`  
`--pgdata=_`directory`_`
    

The target directory that contains a cluster directory from a physical replica. 

`-n`  
`--dry-run`
    

Do everything except actually modifying the target directory. 

`-p _`port`_`  
`--subscriber-port=_`port`_`
    

The port number on which the target server is listening for connections. Defaults to running the target server on port 50432 to avoid unintended client connections. 

`-P _`connstr`_`  
`--publisher-server=_`connstr`_`
    

The connection string to the publisher. For details see [Section 32.1.1](libpq-connect.md#LIBPQ-CONNSTRING "32.1.1. Connection Strings"). 

`-s _`dir`_`  
`--socketdir=_`dir`_`
    

The directory to use for postmaster sockets on target server. The default is current directory. 

`-t _`seconds`_`  
`--recovery-timeout=_`seconds`_`
    

The maximum number of seconds to wait for recovery to end. Setting to 0 disables. The default is 0. 

`-U _`username`_`  
`--subscriber-username=_`username`_`
    

The user name to connect as on target server. Defaults to the current operating system user name. 

`-v`  
`--verbose`
    

Enables verbose mode. This will cause pg_createsubscriber to output progress messages and detailed information about each step to standard error. Repeating the option causes additional debug-level messages to appear on standard error. 

`--config-file=_`filename`_`
    

Use the specified main server configuration file for the target data directory. pg_createsubscriber internally uses the pg_ctl command to start and stop the target server. It allows you to specify the actual `postgresql.conf` configuration file if it is stored outside the data directory. 

`--publication=_`name`_`
    

The publication name to set up the logical replication. Multiple publications can be specified by writing multiple `--publication` switches. The number of publication names must match the number of specified databases, otherwise an error is reported. The order of the multiple publication name switches must match the order of database switches. If this option is not specified, a generated name is assigned to the publication name. 

`--replication-slot=_`name`_`
    

The replication slot name to set up the logical replication. Multiple replication slots can be specified by writing multiple `--replication-slot` switches. The number of replication slot names must match the number of specified databases, otherwise an error is reported. The order of the multiple replication slot name switches must match the order of database switches. If this option is not specified, the subscription name is assigned to the replication slot name. 

`--subscription=_`name`_`
    

The subscription name to set up the logical replication. Multiple subscriptions can be specified by writing multiple `--subscription` switches. The number of subscription names must match the number of specified databases, otherwise an error is reported. The order of the multiple subscription name switches must match the order of database switches. If this option is not specified, a generated name is assigned to the subscription name. 

`-V`  
`--version`
    

Print the pg_createsubscriber version and exit. 

`-?`  
`--help`
    

Show help about pg_createsubscriber command line arguments, and exit. 

## Notes

### Prerequisites

There are some prerequisites for pg_createsubscriber to convert the target server into a logical replica. If these are not met, an error will be reported. The source and target servers must have the same major version as the pg_createsubscriber. The given target data directory must have the same system identifier as the source data directory. The given database user for the target data directory must have privileges for creating [subscriptions](sql-createsubscription.md "CREATE SUBSCRIPTION") and using [`pg_replication_origin_advance()`](functions-admin.md#PG-REPLICATION-ORIGIN-ADVANCE). 

The target server must be used as a physical standby. The target server must have [max_replication_slots](runtime-config-replication.md#GUC-MAX-REPLICATION-SLOTS) and [max_logical_replication_workers](runtime-config-replication.md#GUC-MAX-LOGICAL-REPLICATION-WORKERS) configured to a value greater than or equal to the number of specified databases. The target server must have [max_worker_processes](runtime-config-resource.md#GUC-MAX-WORKER-PROCESSES) configured to a value greater than the number of specified databases. The target server must accept local connections. 

The source server must accept connections from the target server. The source server must not be in recovery. The source server must have [wal_level](runtime-config-wal.md#GUC-WAL-LEVEL) as `logical`. The source server must have [max_replication_slots](runtime-config-replication.md#GUC-MAX-REPLICATION-SLOTS) configured to a value greater than or equal to the number of specified databases plus existing replication slots. The source server must have [max_wal_senders](runtime-config-replication.md#GUC-MAX-WAL-SENDERS) configured to a value greater than or equal to the number of specified databases and existing WAL sender processes. 

### Warnings

If pg_createsubscriber fails after the target server was promoted, then the data directory is likely not in a state that can be recovered. In such case, creating a new standby server is recommended. 

pg_createsubscriber usually starts the target server with different connection settings during transformation. Hence, connections to the target server should fail. 

Since DDL commands are not replicated by logical replication, avoid executing DDL commands that change the database schema while running pg_createsubscriber. If the target server has already been converted to logical replica, the DDL commands might not be replicated, which might cause an error. 

If pg_createsubscriber fails while processing, objects (publications, replication slots) created on the source server are removed. The removal might fail if the target server cannot connect to the source server. In such a case, a warning message will inform the objects left. If the target server is running, it will be stopped. 

If the replication is using [primary_slot_name](runtime-config-replication.md#GUC-PRIMARY-SLOT-NAME), it will be removed from the source server after the logical replication setup. 

If the target server is a synchronous replica, transaction commits on the primary might wait for replication while running pg_createsubscriber. 

pg_createsubscriber sets up logical replication with two-phase commit disabled. This means that any prepared transactions will be replicated at the time of `COMMIT PREPARED`, without advance preparation. Once setup is complete, you can manually drop and re-create the subscription(s) with the [`two_phase`](sql-createsubscription.md#SQL-CREATESUBSCRIPTION-PARAMS-WITH-TWO-PHASE) option enabled. 

pg_createsubscriber changes the system identifier using pg_resetwal. It would avoid situations in which the target server might use WAL files from the source server. If the target server has a standby, replication will break and a fresh standby should be created. 

### How It Works

The basic idea is to have a replication start point from the source server and set up a logical replication to start from this point: 

  1. Start the target server with the specified command-line options. If the target server is already running, pg_createsubscriber will terminate with an error. 

  2. Check if the target server can be converted. There are also a few checks on the source server. If any of the prerequisites are not met, pg_createsubscriber will terminate with an error. 

  3. Create a publication and replication slot for each specified database on the source server. Each publication is created using [`FOR ALL TABLES`](sql-createpublication.md#SQL-CREATEPUBLICATION-PARAMS-FOR-ALL-TABLES). If the `--publication` option is not specified, the publication has the following name pattern: “`pg_createsubscriber_%u_%x`” (parameter: database _`oid`_ , random _`int`_). If the `--replication-slot` option is not specified, the replication slot has the following name pattern: “`pg_createsubscriber_%u_%x`” (parameters: database _`oid`_ , random _`int`_). These replication slots will be used by the subscriptions in a future step. The last replication slot LSN is used as a stopping point in the [recovery_target_lsn](runtime-config-wal.md#GUC-RECOVERY-TARGET-LSN) parameter and by the subscriptions as a replication start point. It guarantees that no transaction will be lost. 

  4. Write recovery parameters into the target data directory and restart the target server. It specifies an LSN ([recovery_target_lsn](runtime-config-wal.md#GUC-RECOVERY-TARGET-LSN)) of the write-ahead log location up to which recovery will proceed. It also specifies `promote` as the action that the server should take once the recovery target is reached. Additional [recovery parameters](runtime-config-wal.md#RUNTIME-CONFIG-WAL-RECOVERY-TARGET "19.5.6. Recovery Target") are added to avoid unexpected behavior during the recovery process such as end of the recovery as soon as a consistent state is reached (WAL should be applied until the replication start location) and multiple recovery targets that can cause a failure. This step finishes once the server ends standby mode and is accepting read-write transactions. If `--recovery-timeout` option is set, pg_createsubscriber terminates if recovery does not end until the given number of seconds. 

  5. Create a subscription for each specified database on the target server. If the `--subscription` option is not specified, the subscription has the following name pattern: “`pg_createsubscriber_%u_%x`” (parameters: database _`oid`_ , random _`int`_). It does not copy existing data from the source server. It does not create a replication slot. Instead, it uses the replication slot that was created in a previous step. The subscription is created but it is not enabled yet. The reason is the replication progress must be set to the replication start point before starting the replication. 

  6. Drop publications on the target server that were replicated because they were created before the replication start location. It has no use on the subscriber. 

  7. Set the replication progress to the replication start point for each subscription. When the target server starts the recovery process, it catches up to the replication start point. This is the exact LSN to be used as a initial replication location for each subscription. The replication origin name is obtained since the subscription was created. The replication origin name and the replication start point are used in [`pg_replication_origin_advance()`](functions-admin.md#PG-REPLICATION-ORIGIN-ADVANCE) to set up the initial replication location. 

  8. Enable the subscription for each specified database on the target server. The subscription starts applying transactions from the replication start point. 

  9. If the standby server was using [primary_slot_name](runtime-config-replication.md#GUC-PRIMARY-SLOT-NAME), it has no use from now on so drop it. 

  10. If the standby server contains [failover replication slots](logicaldecoding-explanation.md#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION "47.2.3. Replication Slot Synchronization"), they cannot be synchronized anymore, so drop them. 

  11. Update the system identifier on the target server. The [pg_resetwal](app-pgresetwal.md "pg_resetwal") is run to modify the system identifier. The target server is stopped as a `pg_resetwal` requirement. 




## Examples

To create a logical replica for databases `hr` and `finance` from a physical replica at `foo`: 
    
    
    $ **pg_createsubscriber -D /usr/local/pgsql/data -P "host=foo" -d hr -d finance**
    

## See Also

[pg_basebackup](app-pgbasebackup.md "pg_basebackup")

* * *

[Prev](app-pgcontroldata.md "pg_controldata") | [Up](reference-server.md "PostgreSQL Server Applications")|  [Next](app-pg-ctl.md "pg_ctl")  
---|---|---  
pg_controldata | [Home](index.md "PostgreSQL 17.5 Documentation")|  pg_ctl

pg_controldata  
---  
[Prev](app-pgchecksums.md "pg_checksums") | [Up](reference-server.md "PostgreSQL Server Applications")| PostgreSQL Server Applications| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](app-pgcreatesubscriber.md "pg_createsubscriber")  
  
* * *

## pg_controldata

pg_controldata — display control information of a PostgreSQL database cluster

## Synopsis

`pg_controldata` [_`option`_] [[ `-D` | `--pgdata` ]_`datadir`_]

## Description

`pg_controldata` prints information initialized during `initdb`, such as the catalog version. It also shows information about write-ahead logging and checkpoint processing. This information is cluster-wide, and not specific to any one database. 

This utility can only be run by the user who initialized the cluster because it requires read access to the data directory. You can specify the data directory on the command line, or use the environment variable `PGDATA`. This utility supports the options `-V` and `--version`, which print the pg_controldata version and exit. It also supports options `-?` and `--help`, which output the supported arguments. 

## Environment

`PGDATA`
    

Default data directory location 

`PG_COLOR`
    

Specifies whether to use color in diagnostic messages. Possible values are `always`, `auto` and `never`. 

* * *

[Prev](app-pgchecksums.md "pg_checksums") | [Up](reference-server.md "PostgreSQL Server Applications")|  [Next](app-pgcreatesubscriber.md "pg_createsubscriber")  
---|---|---  
pg_checksums | [Home](index.md "PostgreSQL 17.5 Documentation")|  pg_createsubscriber

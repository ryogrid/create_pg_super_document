CREATE TABLESPACE  
---  
[Prev](sql-createtableas.md "CREATE TABLE AS") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-createtsconfig.md "CREATE TEXT SEARCH CONFIGURATION")  
  
* * *

## CREATE TABLESPACE

CREATE TABLESPACE — define a new tablespace

## Synopsis
    
    
    CREATE TABLESPACE _tablespace_name_
        [ OWNER { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER } ]
        LOCATION '_directory_ '
        [ WITH ( _tablespace_option_ = _value_ [, ... ] ) ]
    

## Description

`CREATE TABLESPACE` registers a new cluster-wide tablespace. The tablespace name must be distinct from the name of any existing tablespace in the database cluster. 

A tablespace allows superusers to define an alternative location on the file system where the data files containing database objects (such as tables and indexes) can reside. 

A user with appropriate privileges can pass _`tablespace_name`_ to `CREATE DATABASE`, `CREATE TABLE`, `CREATE INDEX` or `ADD CONSTRAINT` to have the data files for these objects stored within the specified tablespace. 

### Warning

A tablespace cannot be used independently of the cluster in which it is defined; see [Section 22.6](manage-ag-tablespaces.md "22.6. Tablespaces"). 

## Parameters

 _`tablespace_name`_
    

The name of a tablespace to be created. The name cannot begin with `pg_`, as such names are reserved for system tablespaces. 

_`user_name`_
    

The name of the user who will own the tablespace. If omitted, defaults to the user executing the command. Only superusers can create tablespaces, but they can assign ownership of tablespaces to non-superusers. 

_`directory`_
    

The directory that will be used for the tablespace. The directory must exist (`CREATE TABLESPACE` will not create it), should be empty, and must be owned by the PostgreSQL system user. The directory must be specified by an absolute path name. 

_`tablespace_option`_
    

A tablespace parameter to be set or reset. Currently, the only available parameters are `seq_page_cost`, `random_page_cost`, `effective_io_concurrency` and `maintenance_io_concurrency`. Setting these values for a particular tablespace will override the planner's usual estimate of the cost of reading pages from tables in that tablespace, and the executor's prefetching behavior, as established by the configuration parameters of the same name (see [seq_page_cost](runtime-config-query.md#GUC-SEQ-PAGE-COST), [random_page_cost](runtime-config-query.md#GUC-RANDOM-PAGE-COST), [effective_io_concurrency](runtime-config-resource.md#GUC-EFFECTIVE-IO-CONCURRENCY), [maintenance_io_concurrency](runtime-config-resource.md#GUC-MAINTENANCE-IO-CONCURRENCY)). This may be useful if one tablespace is located on a disk which is faster or slower than the remainder of the I/O subsystem. 

## Notes

`CREATE TABLESPACE` cannot be executed inside a transaction block. 

## Examples

To create a tablespace `dbspace` at file system location `/data/dbs`, first create the directory using operating system facilities and set the correct ownership: 
    
    
    mkdir /data/dbs
    chown postgres:postgres /data/dbs
    

Then issue the tablespace creation command inside PostgreSQL: 
    
    
    CREATE TABLESPACE dbspace LOCATION '/data/dbs';
    

To create a tablespace owned by a different database user, use a command like this: 
    
    
    CREATE TABLESPACE indexspace OWNER genevieve LOCATION '/data/indexes';
    

## Compatibility

`CREATE TABLESPACE` is a PostgreSQL extension. 

## See Also

[CREATE DATABASE](sql-createdatabase.md "CREATE DATABASE"), [CREATE TABLE](sql-createtable.md "CREATE TABLE"), [CREATE INDEX](sql-createindex.md "CREATE INDEX"), [DROP TABLESPACE](sql-droptablespace.md "DROP TABLESPACE"), [ALTER TABLESPACE](sql-altertablespace.md "ALTER TABLESPACE")

* * *

[Prev](sql-createtableas.md "CREATE TABLE AS") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-createtsconfig.md "CREATE TEXT SEARCH CONFIGURATION")  
---|---|---  
CREATE TABLE AS | [Home](index.md "PostgreSQL 17.5 Documentation")|  CREATE TEXT SEARCH CONFIGURATION

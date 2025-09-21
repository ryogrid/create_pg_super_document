ALTER TABLESPACE  
---  
[Prev](sql-altertable.md "ALTER TABLE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altertsconfig.md "ALTER TEXT SEARCH CONFIGURATION")  
  
* * *

## ALTER TABLESPACE

ALTER TABLESPACE — change the definition of a tablespace

## Synopsis
    
    
    ALTER TABLESPACE _name_ RENAME TO _new_name_
    ALTER TABLESPACE _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER TABLESPACE _name_ SET ( _tablespace_option_ = _value_ [, ... ] )
    ALTER TABLESPACE _name_ RESET ( _tablespace_option_ [, ... ] )
    

## Description

`ALTER TABLESPACE` can be used to change the definition of a tablespace. 

You must own the tablespace to change the definition of a tablespace. To alter the owner, you must also be able to `SET ROLE` to the new owning role. (Note that superusers have these privileges automatically.) 

## Parameters

 _`name`_
    

The name of an existing tablespace. 

_`new_name`_
    

The new name of the tablespace. The new name cannot begin with `pg_`, as such names are reserved for system tablespaces. 

_`new_owner`_
    

The new owner of the tablespace. 

_`tablespace_option`_
    

A tablespace parameter to be set or reset. Currently, the only available parameters are `seq_page_cost`, `random_page_cost`, `effective_io_concurrency` and `maintenance_io_concurrency`. Setting these values for a particular tablespace will override the planner's usual estimate of the cost of reading pages from tables in that tablespace, and the executor's prefetching behavior, as established by the configuration parameters of the same name (see [seq_page_cost](runtime-config-query.md#GUC-SEQ-PAGE-COST), [random_page_cost](runtime-config-query.md#GUC-RANDOM-PAGE-COST), [effective_io_concurrency](runtime-config-resource.md#GUC-EFFECTIVE-IO-CONCURRENCY), [maintenance_io_concurrency](runtime-config-resource.md#GUC-MAINTENANCE-IO-CONCURRENCY)). This may be useful if one tablespace is located on a disk which is faster or slower than the remainder of the I/O subsystem. 

## Examples

Rename tablespace `index_space` to `fast_raid`: 
    
    
    ALTER TABLESPACE index_space RENAME TO fast_raid;
    

Change the owner of tablespace `index_space`: 
    
    
    ALTER TABLESPACE index_space OWNER TO mary;
    

## Compatibility

There is no `ALTER TABLESPACE` statement in the SQL standard. 

## See Also

[CREATE TABLESPACE](sql-createtablespace.md "CREATE TABLESPACE"), [DROP TABLESPACE](sql-droptablespace.md "DROP TABLESPACE")

* * *

[Prev](sql-altertable.md "ALTER TABLE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altertsconfig.md "ALTER TEXT SEARCH CONFIGURATION")  
---|---|---  
ALTER TABLE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER TEXT SEARCH CONFIGURATION

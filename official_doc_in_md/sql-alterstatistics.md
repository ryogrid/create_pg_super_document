ALTER STATISTICS  
---  
[Prev](sql-alterserver.md "ALTER SERVER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altersubscription.md "ALTER SUBSCRIPTION")  
  
* * *

## ALTER STATISTICS

ALTER STATISTICS — change the definition of an extended statistics object 

## Synopsis
    
    
    ALTER STATISTICS _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER STATISTICS _name_ RENAME TO _new_name_
    ALTER STATISTICS _name_ SET SCHEMA _new_schema_
    ALTER STATISTICS _name_ SET STATISTICS { _new_target_ | DEFAULT }
    

## Description

`ALTER STATISTICS` changes the parameters of an existing extended statistics object. Any parameters not specifically set in the `ALTER STATISTICS` command retain their prior settings. 

You must own the statistics object to use `ALTER STATISTICS`. To change a statistics object's schema, you must also have `CREATE` privilege on the new schema. To alter the owner, you must be able to `SET ROLE` to the new owning role, and that role must have `CREATE` privilege on the statistics object's schema. (These restrictions enforce that altering the owner doesn't do anything you couldn't do by dropping and recreating the statistics object. However, a superuser can alter ownership of any statistics object anyway.) 

## Parameters

_`name`_
    

The name (optionally schema-qualified) of the statistics object to be altered. 

_`new_owner`_
    

The user name of the new owner of the statistics object. 

_`new_name`_
    

The new name for the statistics object. 

_`new_schema`_
    

The new schema for the statistics object. 

_`new_target`_
    

The statistic-gathering target for this statistics object for subsequent [`ANALYZE`](sql-analyze.md "ANALYZE") operations. The target can be set in the range 0 to 10000. Set it to `DEFAULT` to revert to using the system default statistics target ([default_statistics_target](runtime-config-query.md#GUC-DEFAULT-STATISTICS-TARGET)). (Setting to a value of -1 is an obsolete way spelling to get the same outcome.) For more information on the use of statistics by the PostgreSQL query planner, refer to [Section 14.2](planner-stats.md "14.2. Statistics Used by the Planner"). 

## Compatibility

There is no `ALTER STATISTICS` command in the SQL standard. 

## See Also

[CREATE STATISTICS](sql-createstatistics.md "CREATE STATISTICS"), [DROP STATISTICS](sql-dropstatistics.md "DROP STATISTICS")

* * *

[Prev](sql-alterserver.md "ALTER SERVER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altersubscription.md "ALTER SUBSCRIPTION")  
---|---|---  
ALTER SERVER | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER SUBSCRIPTION

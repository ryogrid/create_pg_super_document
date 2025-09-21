DROP STATISTICS  
---  
[Prev](sql-dropserver.md "DROP SERVER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropsubscription.md "DROP SUBSCRIPTION")  
  
* * *

## DROP STATISTICS

DROP STATISTICS — remove extended statistics

## Synopsis
    
    
    DROP STATISTICS [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP STATISTICS` removes statistics object(s) from the database. Only the statistics object's owner, the schema owner, or a superuser can drop a statistics object. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the statistics object does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of the statistics object to drop. 

`CASCADE`  
`RESTRICT`
    

These key words do not have any effect, since there are no dependencies on statistics. 

## Examples

To destroy two statistics objects in different schemas, without failing if they don't exist: 
    
    
    DROP STATISTICS IF EXISTS
        accounting.users_uid_creation,
        public.grants_user_role;
    

## Compatibility

There is no `DROP STATISTICS` command in the SQL standard. 

## See Also

[ALTER STATISTICS](sql-alterstatistics.md "ALTER STATISTICS"), [CREATE STATISTICS](sql-createstatistics.md "CREATE STATISTICS")

* * *

[Prev](sql-dropserver.md "DROP SERVER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropsubscription.md "DROP SUBSCRIPTION")  
---|---|---  
DROP SERVER | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP SUBSCRIPTION

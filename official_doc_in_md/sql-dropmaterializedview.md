DROP MATERIALIZED VIEW  
---  
[Prev](sql-droplanguage.md "DROP LANGUAGE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropoperator.md "DROP OPERATOR")  
  
* * *

## DROP MATERIALIZED VIEW

DROP MATERIALIZED VIEW — remove a materialized view

## Synopsis
    
    
    DROP MATERIALIZED VIEW [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP MATERIALIZED VIEW` drops an existing materialized view. To execute this command you must be the owner of the materialized view. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the materialized view does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of the materialized view to remove. 

`CASCADE`
    

Automatically drop objects that depend on the materialized view (such as other materialized views, or regular views), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the materialized view if any objects depend on it. This is the default. 

## Examples

This command will remove the materialized view called `order_summary`: 
    
    
    DROP MATERIALIZED VIEW order_summary;
    

## Compatibility

`DROP MATERIALIZED VIEW` is a PostgreSQL extension. 

## See Also

[CREATE MATERIALIZED VIEW](sql-creatematerializedview.md "CREATE MATERIALIZED VIEW"), [ALTER MATERIALIZED VIEW](sql-altermaterializedview.md "ALTER MATERIALIZED VIEW"), [REFRESH MATERIALIZED VIEW](sql-refreshmaterializedview.md "REFRESH MATERIALIZED VIEW")

* * *

[Prev](sql-droplanguage.md "DROP LANGUAGE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropoperator.md "DROP OPERATOR")  
---|---|---  
DROP LANGUAGE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP OPERATOR

DROP RULE  
---  
[Prev](sql-droproutine.md "DROP ROUTINE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropschema.md "DROP SCHEMA")  
  
* * *

## DROP RULE

DROP RULE — remove a rewrite rule

## Synopsis
    
    
    DROP RULE [ IF EXISTS ] _name_ ON _table_name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP RULE` drops a rewrite rule. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the rule does not exist. A notice is issued in this case. 

_`name`_
    

The name of the rule to drop. 

_`table_name`_
    

The name (optionally schema-qualified) of the table or view that the rule applies to. 

`CASCADE`
    

Automatically drop objects that depend on the rule, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the rule if any objects depend on it. This is the default. 

## Examples

To drop the rewrite rule `newrule`: 
    
    
    DROP RULE newrule ON mytable;
    

## Compatibility

`DROP RULE` is a PostgreSQL language extension, as is the entire query rewrite system. 

## See Also

[CREATE RULE](sql-createrule.md "CREATE RULE"), [ALTER RULE](sql-alterrule.md "ALTER RULE")

* * *

[Prev](sql-droproutine.md "DROP ROUTINE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropschema.md "DROP SCHEMA")  
---|---|---  
DROP ROUTINE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP SCHEMA

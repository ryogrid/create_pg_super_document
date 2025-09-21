ALTER RULE  
---  
[Prev](sql-alterroutine.md "ALTER ROUTINE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterschema.md "ALTER SCHEMA")  
  
* * *

## ALTER RULE

ALTER RULE — change the definition of a rule

## Synopsis
    
    
    ALTER RULE _name_ ON _table_name_ RENAME TO _new_name_
    

## Description

`ALTER RULE` changes properties of an existing rule. Currently, the only available action is to change the rule's name. 

To use `ALTER RULE`, you must own the table or view that the rule applies to. 

## Parameters

 _`name`_
    

The name of an existing rule to alter. 

_`table_name`_
    

The name (optionally schema-qualified) of the table or view that the rule applies to. 

_`new_name`_
    

The new name for the rule. 

## Examples

To rename an existing rule: 
    
    
    ALTER RULE notify_all ON emp RENAME TO notify_me;
    

## Compatibility

`ALTER RULE` is a PostgreSQL language extension, as is the entire query rewrite system. 

## See Also

[CREATE RULE](sql-createrule.md "CREATE RULE"), [DROP RULE](sql-droprule.md "DROP RULE")

* * *

[Prev](sql-alterroutine.md "ALTER ROUTINE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterschema.md "ALTER SCHEMA")  
---|---|---  
ALTER ROUTINE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER SCHEMA

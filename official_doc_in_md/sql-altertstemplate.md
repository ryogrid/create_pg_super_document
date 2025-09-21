ALTER TEXT SEARCH TEMPLATE  
---  
[Prev](sql-altertsparser.md "ALTER TEXT SEARCH PARSER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altertrigger.md "ALTER TRIGGER")  
  
* * *

## ALTER TEXT SEARCH TEMPLATE

ALTER TEXT SEARCH TEMPLATE — change the definition of a text search template

## Synopsis
    
    
    ALTER TEXT SEARCH TEMPLATE _name_ RENAME TO _new_name_
    ALTER TEXT SEARCH TEMPLATE _name_ SET SCHEMA _new_schema_
    

## Description

`ALTER TEXT SEARCH TEMPLATE` changes the definition of a text search template. Currently, the only supported functionality is to change the template's name. 

You must be a superuser to use `ALTER TEXT SEARCH TEMPLATE`. 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing text search template. 

_`new_name`_
    

The new name of the text search template. 

_`new_schema`_
    

The new schema for the text search template. 

## Compatibility

There is no `ALTER TEXT SEARCH TEMPLATE` statement in the SQL standard. 

## See Also

[CREATE TEXT SEARCH TEMPLATE](sql-createtstemplate.md "CREATE TEXT SEARCH TEMPLATE"), [DROP TEXT SEARCH TEMPLATE](sql-droptstemplate.md "DROP TEXT SEARCH TEMPLATE")

* * *

[Prev](sql-altertsparser.md "ALTER TEXT SEARCH PARSER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altertrigger.md "ALTER TRIGGER")  
---|---|---  
ALTER TEXT SEARCH PARSER | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER TRIGGER

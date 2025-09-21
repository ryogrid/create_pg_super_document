ALTER TEXT SEARCH PARSER  
---  
[Prev](sql-altertsdictionary.md "ALTER TEXT SEARCH DICTIONARY") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altertstemplate.md "ALTER TEXT SEARCH TEMPLATE")  
  
* * *

## ALTER TEXT SEARCH PARSER

ALTER TEXT SEARCH PARSER — change the definition of a text search parser

## Synopsis
    
    
    ALTER TEXT SEARCH PARSER _name_ RENAME TO _new_name_
    ALTER TEXT SEARCH PARSER _name_ SET SCHEMA _new_schema_
    

## Description

`ALTER TEXT SEARCH PARSER` changes the definition of a text search parser. Currently, the only supported functionality is to change the parser's name. 

You must be a superuser to use `ALTER TEXT SEARCH PARSER`. 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing text search parser. 

_`new_name`_
    

The new name of the text search parser. 

_`new_schema`_
    

The new schema for the text search parser. 

## Compatibility

There is no `ALTER TEXT SEARCH PARSER` statement in the SQL standard. 

## See Also

[CREATE TEXT SEARCH PARSER](sql-createtsparser.md "CREATE TEXT SEARCH PARSER"), [DROP TEXT SEARCH PARSER](sql-droptsparser.md "DROP TEXT SEARCH PARSER")

* * *

[Prev](sql-altertsdictionary.md "ALTER TEXT SEARCH DICTIONARY") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altertstemplate.md "ALTER TEXT SEARCH TEMPLATE")  
---|---|---  
ALTER TEXT SEARCH DICTIONARY | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER TEXT SEARCH TEMPLATE

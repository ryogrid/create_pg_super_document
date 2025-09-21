DROP TEXT SEARCH TEMPLATE  
---  
[Prev](sql-droptsparser.md "DROP TEXT SEARCH PARSER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droptransform.md "DROP TRANSFORM")  
  
* * *

## DROP TEXT SEARCH TEMPLATE

DROP TEXT SEARCH TEMPLATE — remove a text search template

## Synopsis
    
    
    DROP TEXT SEARCH TEMPLATE [ IF EXISTS ] _name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP TEXT SEARCH TEMPLATE` drops an existing text search template. You must be a superuser to use this command. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the text search template does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of an existing text search template. 

`CASCADE`
    

Automatically drop objects that depend on the text search template, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the text search template if any objects depend on it. This is the default. 

## Examples

Remove the text search template `thesaurus`: 
    
    
    DROP TEXT SEARCH TEMPLATE thesaurus;
    

This command will not succeed if there are any existing text search dictionaries that use the template. Add `CASCADE` to drop such dictionaries along with the template. 

## Compatibility

There is no `DROP TEXT SEARCH TEMPLATE` statement in the SQL standard. 

## See Also

[ALTER TEXT SEARCH TEMPLATE](sql-altertstemplate.md "ALTER TEXT SEARCH TEMPLATE"), [CREATE TEXT SEARCH TEMPLATE](sql-createtstemplate.md "CREATE TEXT SEARCH TEMPLATE")

* * *

[Prev](sql-droptsparser.md "DROP TEXT SEARCH PARSER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droptransform.md "DROP TRANSFORM")  
---|---|---  
DROP TEXT SEARCH PARSER | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP TRANSFORM

DROP TEXT SEARCH DICTIONARY  
---  
[Prev](sql-droptsconfig.md "DROP TEXT SEARCH CONFIGURATION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droptsparser.md "DROP TEXT SEARCH PARSER")  
  
* * *

## DROP TEXT SEARCH DICTIONARY

DROP TEXT SEARCH DICTIONARY — remove a text search dictionary

## Synopsis
    
    
    DROP TEXT SEARCH DICTIONARY [ IF EXISTS ] _name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP TEXT SEARCH DICTIONARY` drops an existing text search dictionary. To execute this command you must be the owner of the dictionary. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the text search dictionary does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of an existing text search dictionary. 

`CASCADE`
    

Automatically drop objects that depend on the text search dictionary, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the text search dictionary if any objects depend on it. This is the default. 

## Examples

Remove the text search dictionary `english`: 
    
    
    DROP TEXT SEARCH DICTIONARY english;
    

This command will not succeed if there are any existing text search configurations that use the dictionary. Add `CASCADE` to drop such configurations along with the dictionary. 

## Compatibility

There is no `DROP TEXT SEARCH DICTIONARY` statement in the SQL standard. 

## See Also

[ALTER TEXT SEARCH DICTIONARY](sql-altertsdictionary.md "ALTER TEXT SEARCH DICTIONARY"), [CREATE TEXT SEARCH DICTIONARY](sql-createtsdictionary.md "CREATE TEXT SEARCH DICTIONARY")

* * *

[Prev](sql-droptsconfig.md "DROP TEXT SEARCH CONFIGURATION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droptsparser.md "DROP TEXT SEARCH PARSER")  
---|---|---  
DROP TEXT SEARCH CONFIGURATION | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP TEXT SEARCH PARSER

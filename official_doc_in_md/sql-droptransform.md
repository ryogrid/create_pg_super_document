DROP TRANSFORM  
---  
[Prev](sql-droptstemplate.md "DROP TEXT SEARCH TEMPLATE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droptrigger.md "DROP TRIGGER")  
  
* * *

## DROP TRANSFORM

DROP TRANSFORM — remove a transform

## Synopsis
    
    
    DROP TRANSFORM [ IF EXISTS ] FOR _type_name_ LANGUAGE _lang_name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP TRANSFORM` removes a previously defined transform. 

To be able to drop a transform, you must own the type and the language. These are the same privileges that are required to create a transform. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the transform does not exist. A notice is issued in this case. 

_`type_name`_
    

The name of the data type of the transform. 

_`lang_name`_
    

The name of the language of the transform. 

`CASCADE`
    

Automatically drop objects that depend on the transform, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the transform if any objects depend on it. This is the default. 

## Examples

To drop the transform for type `hstore` and language `plpython3u`: 
    
    
    DROP TRANSFORM FOR hstore LANGUAGE plpython3u;
    

## Compatibility

This form of `DROP TRANSFORM` is a PostgreSQL extension. See [CREATE TRANSFORM](sql-createtransform.md "CREATE TRANSFORM") for details. 

## See Also

[CREATE TRANSFORM](sql-createtransform.md "CREATE TRANSFORM")

* * *

[Prev](sql-droptstemplate.md "DROP TEXT SEARCH TEMPLATE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droptrigger.md "DROP TRIGGER")  
---|---|---  
DROP TEXT SEARCH TEMPLATE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP TRIGGER

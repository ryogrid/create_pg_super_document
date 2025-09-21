ALTER CONVERSION  
---  
[Prev](sql-altercollation.md "ALTER COLLATION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterdatabase.md "ALTER DATABASE")  
  
* * *

## ALTER CONVERSION

ALTER CONVERSION — change the definition of a conversion

## Synopsis
    
    
    ALTER CONVERSION _name_ RENAME TO _new_name_
    ALTER CONVERSION _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER CONVERSION _name_ SET SCHEMA _new_schema_
    

## Description

`ALTER CONVERSION` changes the definition of a conversion. 

You must own the conversion to use `ALTER CONVERSION`. To alter the owner, you must be able to `SET ROLE` to the new owning role, and that role must have `CREATE` privilege on the conversion's schema. (These restrictions enforce that altering the owner doesn't do anything you couldn't do by dropping and recreating the conversion. However, a superuser can alter ownership of any conversion anyway.) 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing conversion. 

_`new_name`_
    

The new name of the conversion. 

_`new_owner`_
    

The new owner of the conversion. 

_`new_schema`_
    

The new schema for the conversion. 

## Examples

To rename the conversion `iso_8859_1_to_utf8` to `latin1_to_unicode`: 
    
    
    ALTER CONVERSION iso_8859_1_to_utf8 RENAME TO latin1_to_unicode;
    

To change the owner of the conversion `iso_8859_1_to_utf8` to `joe`: 
    
    
    ALTER CONVERSION iso_8859_1_to_utf8 OWNER TO joe;
    

## Compatibility

There is no `ALTER CONVERSION` statement in the SQL standard. 

## See Also

[CREATE CONVERSION](sql-createconversion.md "CREATE CONVERSION"), [DROP CONVERSION](sql-dropconversion.md "DROP CONVERSION")

* * *

[Prev](sql-altercollation.md "ALTER COLLATION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterdatabase.md "ALTER DATABASE")  
---|---|---  
ALTER COLLATION | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER DATABASE

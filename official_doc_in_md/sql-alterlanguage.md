ALTER LANGUAGE  
---  
[Prev](sql-alterindex.md "ALTER INDEX") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterlargeobject.md "ALTER LARGE OBJECT")  
  
* * *

## ALTER LANGUAGE

ALTER LANGUAGE — change the definition of a procedural language

## Synopsis
    
    
    ALTER [ PROCEDURAL ] LANGUAGE _name_ RENAME TO _new_name_
    ALTER [ PROCEDURAL ] LANGUAGE _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    

## Description

`ALTER LANGUAGE` changes the definition of a procedural language. The only functionality is to rename the language or assign a new owner. You must be superuser or owner of the language to use `ALTER LANGUAGE`. 

## Parameters

 _`name`_
    

Name of a language 

_`new_name`_
    

The new name of the language 

_`new_owner`_
    

The new owner of the language 

## Compatibility

There is no `ALTER LANGUAGE` statement in the SQL standard. 

## See Also

[CREATE LANGUAGE](sql-createlanguage.md "CREATE LANGUAGE"), [DROP LANGUAGE](sql-droplanguage.md "DROP LANGUAGE")

* * *

[Prev](sql-alterindex.md "ALTER INDEX") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterlargeobject.md "ALTER LARGE OBJECT")  
---|---|---  
ALTER INDEX | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER LARGE OBJECT

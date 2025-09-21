ALTER SCHEMA  
---  
[Prev](sql-alterrule.md "ALTER RULE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altersequence.md "ALTER SEQUENCE")  
  
* * *

## ALTER SCHEMA

ALTER SCHEMA — change the definition of a schema

## Synopsis
    
    
    ALTER SCHEMA _name_ RENAME TO _new_name_
    ALTER SCHEMA _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    

## Description

`ALTER SCHEMA` changes the definition of a schema. 

You must own the schema to use `ALTER SCHEMA`. To rename a schema you must also have the `CREATE` privilege for the database. To alter the owner, you must be able to `SET ROLE` to the new owning role, and that role must have the `CREATE` privilege for the database. (Note that superusers have all these privileges automatically.) 

## Parameters

 _`name`_
    

The name of an existing schema. 

_`new_name`_
    

The new name of the schema. The new name cannot begin with `pg_`, as such names are reserved for system schemas. 

_`new_owner`_
    

The new owner of the schema. 

## Compatibility

There is no `ALTER SCHEMA` statement in the SQL standard. 

## See Also

[CREATE SCHEMA](sql-createschema.md "CREATE SCHEMA"), [DROP SCHEMA](sql-dropschema.md "DROP SCHEMA")

* * *

[Prev](sql-alterrule.md "ALTER RULE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altersequence.md "ALTER SEQUENCE")  
---|---|---  
ALTER RULE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER SEQUENCE

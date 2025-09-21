ALTER LARGE OBJECT  
---  
[Prev](sql-alterlanguage.md "ALTER LANGUAGE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altermaterializedview.md "ALTER MATERIALIZED VIEW")  
  
* * *

## ALTER LARGE OBJECT

ALTER LARGE OBJECT — change the definition of a large object

## Synopsis
    
    
    ALTER LARGE OBJECT _large_object_oid_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    

## Description

`ALTER LARGE OBJECT` changes the definition of a large object. 

You must own the large object to use `ALTER LARGE OBJECT`. To alter the owner, you must also be able to `SET ROLE` to the new owning role. (However, a superuser can alter any large object anyway.) Currently, the only functionality is to assign a new owner, so both restrictions always apply. 

## Parameters

 _`large_object_oid`_
    

OID of the large object to be altered 

_`new_owner`_
    

The new owner of the large object 

## Compatibility

There is no `ALTER LARGE OBJECT` statement in the SQL standard. 

## See Also

[Chapter 33](largeobjects.md "Chapter 33. Large Objects")

* * *

[Prev](sql-alterlanguage.md "ALTER LANGUAGE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altermaterializedview.md "ALTER MATERIALIZED VIEW")  
---|---|---  
ALTER LANGUAGE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER MATERIALIZED VIEW

LOAD  
---  
[Prev](sql-listen.md "LISTEN") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-lock.md "LOCK")  
  
* * *

## LOAD

LOAD — load a shared library file

## Synopsis
    
    
    LOAD '_filename_ '
    

## Description

This command loads a shared library file into the PostgreSQL server's address space. If the file has been loaded already, the command does nothing. Shared library files that contain C functions are automatically loaded whenever one of their functions is called. Therefore, an explicit `LOAD` is usually only needed to load a library that modifies the server's behavior through “hooks” rather than providing a set of functions. 

The library file name is typically given as just a bare file name, which is sought in the server's library search path (set by [dynamic_library_path](runtime-config-client.md#GUC-DYNAMIC-LIBRARY-PATH)). Alternatively it can be given as a full path name. In either case the platform's standard shared library file name extension may be omitted. See [Section 36.10.1](xfunc-c.md#XFUNC-C-DYNLOAD "36.10.1. Dynamic Loading") for more information on this topic. 

Non-superusers can only apply `LOAD` to library files located in `$libdir/plugins/` — the specified _`filename`_ must begin with exactly that string. (It is the database administrator's responsibility to ensure that only “safe” libraries are installed there.) 

## Compatibility

`LOAD` is a PostgreSQL extension. 

## See Also

[CREATE FUNCTION](sql-createfunction.md "CREATE FUNCTION")

* * *

[Prev](sql-listen.md "LISTEN") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-lock.md "LOCK")  
---|---|---  
LISTEN | [Home](index.md "PostgreSQL 17.5 Documentation")|  LOCK

# _defaultACLInfo

## Location
[src/bin/pg_dump/pg_dump.h:586-591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L586-L591)

## Overview
The `_defaultACLInfo` structure represents default access control list (ACL) information for database objects in pg_dump, used to store and manage default privilege settings during database dump operations.

## Definition
```c
typedef struct _defaultACLInfo
{
    DumpableObject dobj;
    DumpableAcl dacl;
    const char *defaclrole;
    char        defaclobjtype;
} DefaultACLInfo;
```

## Detailed Description
This structure is part of pg_dump's internal representation of default access control lists. Default ACLs in PostgreSQL allow database administrators to set up default privileges that will be applied to objects of specific types when they are created by particular roles. This structure captures the metadata needed to recreate these default privilege settings during database restore operations.

## Parameters / Member Variables
- `dobj`: Base dumpable object information containing catalog ID, name, and dump ordering details
- `dacl`: Access control list information representing the default privileges to be granted
- `defaclrole`: Name of the role for which these default privileges are defined
- `defaclobjtype`: Character code representing the type of objects these default privileges apply to (e.g., 'r' for relations, 'f' for functions, 'S' for sequences, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This structure is defined in pg_dump.h as part of the pg_dump utility's internal data structures
- The typedef creates an alias `DefaultACLInfo` for easier reference throughout the codebase
- Default ACLs are a PostgreSQL feature that allows setting privileges that will be automatically applied to newly created objects
- The `defaclobjtype` field uses single character codes to identify different object types, following PostgreSQL's internal conventions
- This structure enables pg_dump to preserve default privilege configurations across database migrations
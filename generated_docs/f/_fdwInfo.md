# _fdwInfo

## Location
[src/bin/pg_dump/pg_dump.h:565-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L565-L572)

## Overview
The  structure represents foreign data wrapper information in the PostgreSQL dump utility, storing metadata about foreign data wrappers for database export operations.

## Definition

```c
typedef struct _fdwInfo
{
	DumpableObject dobj;
	DumpableAcl dacl;
	const char *rolname;
	char	   *fdwhandler;
	char	   *fdwvalidator;
	char	   *fdwoptions;
} FdwInfo;
```
## Detailed Description
This structure is part of the pg_dump utility's internal representation of database objects. It stores information about foreign data wrappers (FDWs), which are PostgreSQL extensions that allow access to data stored in external data sources. FDWs enable PostgreSQL to query and manipulate data from remote databases, files, web services, and other external systems as if they were local tables.

## Parameters / Member Variables
- : Base  structure containing common dump metadata (name, namespace, dependencies, etc.)
- :  structure containing access control list (permission) information for this FDW
- : Name of the role (user) that owns this foreign data wrapper
- : Name of the handler function that implements the FDW's core functionality
- : Name of the validator function that validates options passed to the FDW
- : String containing FDW-specific options and their values

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- This structure is defined in  at lines 565-572
- Part of PostgreSQL's foreign data wrapper infrastructure support in pg_dump
- The handler function is required and implements the FDW's data access methods
- The validator function is optional and validates FDW and foreign server options
- fdwoptions stores configuration parameters specific to the FDW implementation
- The structure inherits from DumpableObject and includes DumpableAcl for comprehensive dump support
- Foreign data wrappers are the foundation for PostgreSQL's SQL/MED (Management of External Data) functionality
# _foreignServerInfo

## Location
[src/bin/pg_dump/pg_dump.h:575-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L575-L583)

## Overview
The  structure represents metadata about a foreign server object in pg_dump, used for storing and managing foreign server information during database dump operations.

## Definition


## Detailed Description
This structure is part of pg_dump's internal representation of database objects. It encapsulates all necessary information about a foreign server that needs to be preserved during dump operations. Foreign servers are PostgreSQL objects that define connections to external data sources through foreign data wrappers (FDW). The structure inherits dumpable object properties and includes access control information along with server-specific attributes.

## Parameters / Member Variables
- : Base dumpable object information containing catalog ID, name, and dump ordering details
- : Access control list information for the foreign server
- : Name of the role/user that owns the foreign server
- : Object ID of the foreign data wrapper associated with this server
- : Optional server type specification as defined by the FDW
- : Optional server version information
- : Server-specific options stored as a formatted string

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This structure is defined in pg_dump.h, indicating it's part of the pg_dump utility's internal data structures
- The typedef creates an alias  for easier reference throughout the codebase
- Foreign servers are part of PostgreSQL's SQL/MED (Management of External Data) implementation
- The structure is designed to capture all metadata needed to recreate foreign server definitions during database restore operations
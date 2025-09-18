# ForeignServerInfo

## Location
[src/bin/pg_dump/pg_dump.h:584-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L584-L585)

## Overview
ForeignServerInfo is a structure used by pg_dump to represent PostgreSQL foreign servers, storing metadata necessary for dumping and restoring foreign server definitions.

## Definition


## Detailed Description
ForeignServerInfo is a data structure that encapsulates all information about PostgreSQL foreign servers needed for backup and restore operations. It extends the standard DumpableObject pattern used throughout pg_dump, allowing foreign servers to be treated as dumpable database objects with proper dependency tracking and selective dumping capabilities. The structure stores both the server's configuration properties and metadata needed for proper access control restoration.

## Parameters / Member Variables
- : Base DumpableObject containing object identification, name, namespace, and dump control flags
- : DumpableAcl structure containing access control list information and default privileges
- : Name of the role (user) who owns this foreign server
- : Object identifier (OID) of the foreign data wrapper that this server uses
- : Optional server type specification as defined when creating the foreign server
- : Optional server version specification as defined when creating the foreign server  
- : String representation of server-specific options in "option=value" format

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - DumpableAcl (ACL handling)
- Called from (representative examples):
  - [getForeignServers](../g/getForeignServers.md)
  - [dumpForeignServer](../d/dumpForeignServer.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- Foreign servers are associated with foreign data wrappers and can have user mappings
- The structure supports the full pg_dump component system including ACLs, comments, and user mappings
- Server options are stored as a formatted string rather than individual fields for flexibility
- Part of PostgreSQL's SQL/MED (Management of External Data) implementation for accessing external data sources
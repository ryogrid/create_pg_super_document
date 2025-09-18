# dumpForeignDataWrapper

## Location
[src/bin/pg_dump/pg_dump.c:14909-14978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14909-L14978)

## Overview
Writes out a single foreign-data wrapper definition to the PostgreSQL dump output, generating the necessary CREATE FOREIGN DATA WRAPPER statement with handler, validator, and options.

## Definition


## Detailed Description
The  function is responsible for dumping foreign-data wrapper objects during a pg_dump operation. It generates the CREATE FOREIGN DATA WRAPPER statement with optional HANDLER and VALIDATOR functions, as well as any wrapper-specific options. Foreign-data wrappers are part of PostgreSQL's foreign data access framework, allowing access to external data sources.

The function constructs the complete CREATE statement by conditionally including handler and validator functions (when not set to "-") and formatting options when present. It also handles binary upgrade scenarios, dumps associated comments, and exports access control lists (ACLs).

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : FdwInfo structure containing foreign-data wrapper metadata including handler, validator, options, and ownership information

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - destroyPQExpBuffer
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - strcmp
  - strlen
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpACL](dumpACL.md)
  - free
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (via switch statement for DO_FDW objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Handler and validator functions are optional - only included if not set to "-"
- Options are formatted as comma-separated key=value pairs within parentheses
- Supports binary upgrade mode with appropriate extension member handling (no namespace)
- Includes owner information in the archive entry for proper ownership restoration
- Comments are dumped without namespace (NULL passed to dumpComment)
- Exports access control lists (ACLs) for permission restoration
- Part of PostgreSQL's foreign data wrapper infrastructure for accessing external data
- Foreign-data wrappers define the interface for connecting to external data sources
- Uses simple quoted names (not schema-qualified) as FDWs are not schema-scoped objects
# AccessMethodInfo

## Location
src/bin/pg_dump/pg_dump.h: 266 - 267

## Overview
AccessMethodInfo is a structure used in pg_dump to represent access method metadata during database dump operations, storing essential information about PostgreSQL access methods including their type and handler function.

## Definition


## Detailed Description
AccessMethodInfo stores metadata about PostgreSQL access methods for the dump and restore process. Access methods define how PostgreSQL stores and retrieves data (like B-tree, Hash, GiST, GIN, etc.). This structure captures the access method's type classification and the name of its handler function, enabling pg_dump to properly recreate custom access methods during database restoration with their original specifications and behavior.

## Parameters / Member Variables
- : DumpableObject containing basic dump metadata (OID, name, namespace, dependencies, etc.)
- : Character indicating the access method type - typically 'i' for index access methods, 't' for table access methods
- : String containing the name of the handler function that implements the access method's interface

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure for dump metadata)

- Called from (representative examples):
  - [getAccessMethods](../g/getAccessMethods.md) (src/bin/pg_dump/pg_dump.c:6246, 6281)
  - [dumpAccessMethod](../d/dumpAccessMethod.md) (src/bin/pg_dump/pg_dump.c:13274)
  - [findAccessMethodByOid](../f/findAccessMethodByOid.md) (src/bin/pg_dump/common.c:952)
  - [selectDumpableAccessMethod](../s/selectDumpableAccessMethod.md) (src/bin/pg_dump/pg_dump.c:2034)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10559)

## Notes and Other Information
- Essential for preserving custom access methods during database migration and backup/restore operations
- The amtype field distinguishes between different categories of access methods (index vs table access methods)
- The amhandler field references the C function that provides the access method's implementation interface
- Access methods are a core extensibility feature in PostgreSQL, allowing custom storage and indexing strategies
- Located in src/bin/pg_dump/pg_dump.h:261-266
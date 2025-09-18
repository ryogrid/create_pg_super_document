# ShellTypeInfo

## Location
src/bin/pg_dump/pg_dump.h: 230 - 231

## Overview
ShellTypeInfo represents a shell (placeholder) type definition in pg_dump, used to create forward declarations for base and range types before their full I/O functions are defined.

## Definition


## Detailed Description
ShellTypeInfo extends DumpableObject to represent shell type definitions during the dump process. Shell types are placeholder type declarations that allow PostgreSQL to resolve dependencies when creating base types and range types that have associated I/O functions or canonicalize functions. The shell type is created first with just "CREATE TYPE typename;", then the full type definition with its functions is created later, ensuring proper dependency ordering.

Shell types are automatically created by getTypes() for base types (typtype='b') and range types (typtype='r') that need to be dumped. They are initially marked as not to be dumped (DUMP_COMPONENT_NONE) and only get marked for dumping if their associated I/O or canonicalize functions need to be dumped, which is determined during dependency sorting.

## Parameters / Member Variables
- : Base DumpableObject containing metadata like dump ID, name, and namespace (note: shell types do not have a catalog ID since they're not real catalog entries)
- : Pointer back to the associated TypeInfo structure that this shell type represents

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [TypeInfo](../T/TypeInfo.md) (associated full type structure)
- Called from (representative examples):
  - [getTypes](../g/getTypes.md) (creates ShellTypeInfo for base and range types)
  - [dumpShellType](../d/dumpShellType.md) (outputs CREATE TYPE shell statements)
  - fmtQualifiedDumpable (formats qualified shell type names)

## Notes and Other Information
- Shell types do not have catalog IDs (catId is set to nilCatalogId) since they're not real catalog entries
- The dump flag is initially set to DUMP_COMPONENT_NONE and only changed during dependency analysis
- Shell types do not have DROP commands - [cleanup](../c/cleanup.md) is handled through the base type
- Owner changes are deferred until after the full type definition to avoid backend complaints
- Essential for handling circular dependencies between types and their I/O functions
- Used in binary upgrade mode to preserve OIDs properly
- Located in src/bin/pg_dump/pg_dump.h:225-230
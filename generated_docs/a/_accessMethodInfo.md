# _accessMethodInfo

## Location
src/bin/pg_dump/pg_dump.h: 261 - 265

## Overview
A structure definition used in PostgreSQL's pg_dump utility to represent access method information for database dumping and restoration operations.

## Definition


## Detailed Description
The  structure is part of PostgreSQL's pg_dump utility framework, designed to store metadata about access methods during database backup operations. This structure extends the base  to include access method-specific information, allowing pg_dump to properly serialize and restore access method definitions. Access methods in PostgreSQL define how data is stored and accessed in indexes and tables.

## Parameters / Member Variables
- : Base  structure containing common metadata for dumpable database objects (object ID, name, namespace, etc.)
- : Character representing the type of access method (e.g., 'i' for index, 't' for table access method)
- : Pointer to string containing the name of the handler function for this access method

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
- Called from (representative examples):
  - [getAccessMethods](../g/getAccessMethods.md) (allocation and initialization)
  - [dumpAccessMethod](../d/dumpAccessMethod.md) (for dumping access method definitions)
  - [selectDumpableAccessMethod](../s/selectDumpableAccessMethod.md) (for determining what to dump)
  - [findAccessMethodByOid](../f/findAccessMethodByOid.md) (for lookup operations)

## Notes and Other Information
- This structure is specifically used within the pg_dump utility context
- The structure is typedef'd as  for easier usage throughout the codebase
- Access methods are relatively new PostgreSQL features that allow pluggable storage and index implementations
- The structure is allocated in arrays when retrieving multiple access methods from the database catalog
- Part of PostgreSQL's extensible architecture allowing custom access methods to be defined and used
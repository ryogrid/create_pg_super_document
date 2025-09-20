# _dumpableObjectWithAcl

## Location
[src/bin/pg_dump/pg_dump.h:172-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L172-L175)

## Overview
The  structure is a generic composite structure that combines the base  with  for any database object type that supports Access Control Lists, providing a convenient way to access both object metadata and ACL information.

## Definition

```c
typedef struct _dumpableObjectWithAcl
{
	DumpableObject dobj;
	DumpableAcl dacl;
} DumpableObjectWithAcl;
```
## Detailed Description
The  structure serves as a generic container that allows pg_dump to uniformly handle any database object that supports ACLs. By combining the base  structure with the  extension, it provides a standardized layout that can be cast to and from specific object types that follow the ACL-enabled object pattern.

This design enables generic ACL processing functions to work with any ACL-enabled object type without needing to know the specific object details, promoting code reuse and maintaining a consistent approach to permission handling across different database object types in pg_dump.

## Parameters / Member Variables
- `dobj`: The base  structure containing core object metadata, identification, and dependency information
- `dacl`: The  structure containing all ACL-related information including current permissions, defaults, and initial privileges
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
- Called from (representative examples):
  - Used as a generic interface for ACL processing functions
  - Cast target for specific object types with ACL support

## Notes and Other Information
This structure implements the pattern required for ACL-enabled objects where the  sub-struct must immediately follow the  base struct. It serves as both a template and a generic interface, allowing functions to work with any ACL-enabled object through casting. The design ensures memory layout compatibility across all object types that support ACLs while providing type safety through the structured approach. This is particularly useful in functions that need to process ACLs for multiple different object types in a uniform manner.
# _shellTypeInfo

## Location
[src/bin/pg_dump/pg_dump.h:225-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L225-L229)

## Overview
The _shellTypeInfo structure represents a "shell type" in PostgreSQL's pg_dump utility, used to handle forward declarations and type dependencies during the dump process.

## Definition

```c
typedef struct _shellTypeInfo
{
	DumpableObject dobj;

	TypeInfo   *baseType;		/* back link to associated base type */
} ShellTypeInfo;
```
## Detailed Description
The _shellTypeInfo structure is a lightweight wrapper used by pg_dump to manage shell types. Shell types are placeholder type definitions that allow PostgreSQL to handle circular dependencies between types and functions. When a type needs to be referenced before it's fully defined (such as in function signatures), a shell type is created first. This structure maintains the basic dumpable object information and a back-reference to the full type definition that will be created later in the dump process.

## Parameters / Member Variables
- : Base dumpable object structure containing common dump metadata including the type name and dump ordering information
- : Pointer back to the associated full _typeInfo structure that this shell type represents

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [TypeInfo](../T/TypeInfo.md)
- Called from (representative examples):
  - [_typeInfo](../t/_typeInfo.md) (referenced via shellType pointer)

## Notes and Other Information
- Shell types are essential for handling PostgreSQL's complex type dependency graphs during dump operations
- The structure is minimal by design, containing only what's necessary for dependency resolution
- The baseType back-link ensures that shell types can be properly resolved to their full type definitions
- Shell types are typically created when a type is needed for a function signature before the type itself has been fully processed
- This mechanism allows pg_dump to maintain proper dependency ordering in the output SQL
# _aggInfo

## Location
[src/bin/pg_dump/pg_dump.h:245-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L245-L248)

## Overview
The _aggInfo structure represents aggregate function metadata used by PostgreSQL's pg_dump utility, extending the basic function information with aggregate-specific properties.

## Definition

```c
typedef struct _aggInfo
{
	FuncInfo	aggfn;
	/* we don't require any other fields at the moment */
} AggInfo;
```
## Detailed Description
The _aggInfo structure is a specialized extension of the _funcInfo structure designed specifically for aggregate functions in PostgreSQL's pg_dump utility. Currently, it serves as a simple wrapper around FuncInfo, indicating that aggregate functions are treated as a special category of functions but don't require additional metadata beyond what regular functions need. The comment suggests that this structure is designed to be extensible, allowing for aggregate-specific fields to be added in the future if needed.

## Parameters / Member Variables
- `aggfn`: A complete FuncInfo structure containing all the standard function metadata including dumpable object information, access control, owner, language, arguments, return type, and postponement flags
## Dependencies
- Functions called/Symbols referenced:
  - FuncInfo (which includes DumpableObject, DumpableAcl, and other function metadata)
- Called from (representative examples):
  - No direct references found (likely used internally by pg_dump aggregate-handling functions)

## Notes and Other Information
- This structure demonstrates PostgreSQL's recognition that aggregates are fundamentally functions with special behavior
- The minimalist design reflects that most aggregate metadata is already captured by the base FuncInfo structure
- The structure is designed for future extensibility if aggregate-specific dump information becomes necessary
- Aggregate functions in PostgreSQL have additional system catalog information (like transition functions, final functions, etc.) that may not need to be tracked separately during dumping
- The inheritance-like design (embedding FuncInfo) allows aggregate functions to be processed by the same code paths as regular functions while maintaining type distinction
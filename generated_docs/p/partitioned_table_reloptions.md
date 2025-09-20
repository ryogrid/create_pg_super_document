# partitioned_table_reloptions

## Location
[src/backend/access/common/reloptions.c:1993-2006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1993-L2006)

## Overview
A specialized function that handles relation options for partitioned tables by rejecting storage parameters and directing users to configure leaf partitions instead.

## Definition

```c
bytea *
partitioned_table_reloptions(Datum reloptions, bool validate)
```
## Detailed Description
This function serves as the relation options parser specifically for partitioned tables. Unlike other relation option parsers, it doesn't actually parse or process any options. Instead, it enforces PostgreSQL's design principle that partitioned tables (parent tables in a partitioning hierarchy) should not have their own storage parameters. When validation is enabled and options are provided, it raises an error directing users to specify storage parameters on the individual leaf partitions rather than the parent partitioned table. When validation is disabled or no options are provided, it simply returns NULL.

## Parameters / Member Variables
- : Input Datum containing relation options (should be empty for partitioned tables)
- : Boolean flag indicating whether to validate and enforce the partitioned table restriction

## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - [errhint](../e/errhint.md) (PostgreSQL error hint function)
  - ERRCODE_WRONG_OBJECT_TYPE (error code constant)
- Called from:
  - [extractRelOptions](../e/extractRelOptions.md) (src/backend/access/common/reloptions.c:1414)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:867)
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md) (src/backend/commands/tablecmds.c:15105)
  - GET_STRING_RELOPTION (src/include/access/reloptions.h:240)

## Notes and Other Information
- Always returns NULL as partitioned tables should not have relation options
- Implements PostgreSQL's partitioning design where storage parameters are specified on leaf partitions, not the parent
- The error message provides clear guidance to users about where storage parameters should be specified
- Used by DDL commands like CREATE TABLE and ALTER TABLE when dealing with partitioned tables
- This function enforces architectural constraints at the relation option parsing level
- The validate parameter controls whether to be strict about rejecting options or silently ignore them
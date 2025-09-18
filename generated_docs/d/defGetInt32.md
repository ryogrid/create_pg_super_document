# defGetInt32

## Location
src/backend/commands/define.c: 162 - 185

## Overview
Extracts a 32-bit signed integer value from a DefElem, accepting only T_Integer node types.

## Definition
```c
int32 defGetInt32(DefElem *def)
```

## Detailed Description
The `defGetInt32` function extracts integer values from DefElem nodes and returns them as 32-bit signed integers (int32). It is the most restrictive of the defGet functions, only accepting T_Integer node types and requiring an explicit argument value. Unlike `defGetBoolean` which defaults to true when no argument is provided, this function reports an error if def->arg is NULL.

The function performs a simple extraction and cast from the internal integer representation to int32, making it suitable for cases where a specific 32-bit integer value is required from SQL definition elements.

## Parameters / Member Variables
- `def`: A pointer to a DefElem structure containing the definition element from which to extract an integer value

## Dependencies
- Functions called/Symbols referenced:
  - DefElem (structure type)
  - nodeTag (for type checking)
  - intVal (to extract integer values and cast to int32)
  - ereport (for error reporting)
  - errcode/errmsg (for error handling)
  
- Called from (representative examples):
  - DefineAggregate (src/backend/commands/aggregatecmds.c:176)
  - DefineAggregate (src/backend/commands/aggregatecmds.c:180)
  - createdb (src/backend/commands/dbcommands.c:880)
  - createdb (src/backend/commands/dbcommands.c:940)
  - AlterDatabase (src/backend/commands/dbcommands.c:2410)
  - ATExecSetIdentity (src/backend/commands/tablecmds.c:8201)
  - ExecVacuum (src/backend/commands/vacuum.c:268)

## Notes and Other Information
- Most restrictive of the defGet functions - only accepts T_Integer node types
- Requires an explicit argument value; reports error if def->arg is NULL
- Returns int32 type, suitable for PostgreSQL's internal 32-bit integer requirements
- The function is located in src/backend/commands/define.c:162-185
- Commonly used in DDL commands where integer parameters with specific size requirements are needed
- Performs explicit cast from internal integer representation to ensure int32 type
- Less commonly used compared to other defGet functions due to its restrictive nature
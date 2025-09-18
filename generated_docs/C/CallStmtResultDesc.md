# CallStmtResultDesc

## Location
src/backend/commands/functioncmds.c: 2365 - 2409

## Overview
Constructs a tuple descriptor for CALL statement results, handling polymorphic types by resolving actual output argument types.

## Definition
```c
TupleDesc CallStmtResultDesc(CallStmt *stmt)
```

## Detailed Description
This function creates a tuple descriptor that describes the result structure of a CALL statement. It starts by building a basic function result tuple descriptor from the procedure's catalog entry, then refines it to handle polymorphic types correctly. The function addresses a key issue: the catalog contains declared output argument types, but for polymorphic procedures, the actual runtime types may differ from the declared types.

The function resolves this by examining the stmt->outargs list, which contains the actual resolved types after type resolution during parsing. It updates each attribute in the tuple descriptor with the correct runtime type while preserving the original attribute names and using standard defaults for attcollation and atttypmod.

## Parameters / Member Variables
- `stmt`: CallStmt node containing the function expression and resolved output arguments

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (procedure catalog lookup)
  - [build_function_result_tupdesc_t](../b/build_function_result_tupdesc_t.md) (initial tuple descriptor creation)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) (attribute initialization)
  - [list_nth](../l/list_nth.md) (output argument access)
  - exprType (type extraction from expressions)
- Called from (representative examples):
  - UtilityTupleDescriptor

## Notes and Other Information
- Returns NULL if the procedure has no output arguments
- Handles polymorphic procedures by using actual resolved types from stmt->outargs
- Preserves original attribute names from the procedure definition
- Sets atttypmod to -1 and uses default collation, which is standard for function outputs
- Part of PostgreSQL's query planning infrastructure for CALL statements
- Critical for proper result set description in tools and client libraries
- Ensures tuple descriptor matches the actual data types that will be returned at runtime
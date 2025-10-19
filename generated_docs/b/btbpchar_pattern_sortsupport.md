# btbpchar_pattern_sortsupport

## Location
[src/backend/utils/adt/varchar.c:1221-1234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L1221-L1234)

## Overview
Provides sort support functionality for BPCHAR (CHAR(n)) data type when using pattern-based comparisons with "C" collation semantics.

## Definition
```c
Datum btbpchar_pattern_sortsupport(PG_FUNCTION_ARGS)
```

## Detailed Description
This function sets up sort support for BPCHAR (fixed-length character) data type specifically for pattern-based operations that require "C" collation behavior. It is part of PostgreSQL's operator class system for B-tree indexes and serves as a sortsupport function for pattern comparison operators.

The function delegates to the generic `varstr_sortsupport` function, explicitly forcing the use of C_COLLATION_OID to ensure consistent binary-based comparison behavior regardless of the database's default collation. This is essential for pattern matching operations where locale-specific collation rules should not interfere with the comparison logic.

The function operates within the appropriate memory context by switching to the SortSupport's context before initializing the sort support structure, ensuring proper memory management during sort operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `ssup`: SortSupport pointer (retrieved via `PG_GETARG_POINTER(0)`) - the sort support structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Memory context management
  - [varstr_sortsupport](../v/varstr_sortsupport.md): Generic string sort support function
  - `PG_GETARG_POINTER`: Argument extraction macro
  - `PG_RETURN_VOID`: Return value macro
- Called from (representative examples):
  - No direct references found (likely registered in operator class definitions)

## Notes and Other Information
- Located in `src/backend/utils/adt/varchar.c:1221-1234`
- Part of the BPCHAR pattern comparison operator infrastructure
- Uses `C_COLLATION_OID` to force binary comparison behavior
- Memory context switching ensures proper allocation scope for sort support structures
- Related to other pattern comparison functions like `bpchar_pattern_gt`, `bpchar_pattern_ge`, `btbpchar_pattern_cmp`
- The "bt" prefix indicates this is specifically for B-tree index support
- Pattern-based operations are typically used with LIKE operators and similar pattern matching functionality

## Simplified Source

```c
Datum btbpchar_pattern_sortsupport(PG_FUNCTION_ARGS) {
    SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
    MemoryContext oldcontext;

    // Switch to sort support memory context
    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    // Use generic string sort support with "C" collation for pattern matching
    varstr_sortsupport(ssup, BPCHAROID, C_COLLATION_OID);

    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}
```
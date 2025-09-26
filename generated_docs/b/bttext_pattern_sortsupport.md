# bttext_pattern_sortsupport

## Location
[src/backend/utils/adt/varlena.c:2899-2921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2899-L2921)

## Overview
A PostgreSQL sort support function that configures optimized sorting for text pattern operations by setting up generic string sorting with "C" collation for B-tree indexes.

## Definition
```c
Datum bttext_pattern_sortsupport(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bttext_pattern_sortsupport` function initializes sort support for text pattern B-tree operations. It serves as a sort support setup function that configures optimized sorting algorithms for text data when used in pattern-matching contexts. The function delegates to the generic string sort support infrastructure (`varstr_sortsupport`) while explicitly forcing the use of "C" collation to ensure consistent byte-wise comparison behavior.

This function is part of PostgreSQL's sort support framework, which provides performance optimizations for sorting operations in B-tree indexes. By using "C" collation, it ensures that pattern-based text comparisons are performed in a consistent, locale-independent manner suitable for pattern matching operations like LIKE clauses.

## Parameters / Member Variables
- `ssup`: SortSupport structure pointer (retrieved using `PG_GETARG_POINTER(0)`) that contains context and configuration for the sort operation

## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (type cast)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [varstr_sortsupport](../v/varstr_sortsupport.md)
  - PG_GETARG_POINTER
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's B-tree operator class infrastructure)

## Notes and Other Information
- [Sort](../S/Sort.md) support function specifically for text pattern B-tree operations
- Forces use of "C" collation (C_COLLATION_OID) to ensure consistent byte-wise comparison
- Uses memory context switching to ensure sort support setup occurs in the appropriate memory context
- Leverages PostgreSQL's generic string sort support infrastructure through `varstr_sortsupport`
- Located in `src/backend/utils/adt/varlena.c` at lines 2899-2921
- Part of the pattern comparison operator class infrastructure for efficient indexing
- Enables performance optimizations for sorting operations in pattern-based indexes
- Essential for LIKE clause performance when using pattern-compatible indexes
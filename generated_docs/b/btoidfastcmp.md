# btoidfastcmp

## Location
[src/backend/access/nbtree/nbtcompare.c:273-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L273-L286)

## Overview
A fast comparison function for OID (Object Identifier) values used within PostgreSQL's sort support framework for optimized B-tree operations.

## Definition
```c
static int btoidfastcmp(Datum x, Datum y, SortSupport ssup)
```

## Detailed Description
The btoidfastcmp function is an optimized comparison function for OID values that operates within PostgreSQL's sort support framework. Unlike the standard btoidcmp function, this version is designed for high-performance sorting operations and takes Datum arguments directly rather than using the PostgreSQL function call interface. It performs the same logical comparison as btoidcmp but with reduced overhead, making it suitable for bulk sorting operations. The function is declared as static, indicating it's only used within the nbtcompare.c module.

## Parameters / Member Variables
- `x`: Datum containing the first OID value to compare
- `y`: Datum containing the second OID value to compare  
- `ssup`: SortSupport structure containing sort configuration (unused in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetObjectId](../D/DatumGetObjectId.md): Extracts OID value from Datum
  - A_GREATER_THAN_B: Constant indicating first argument is greater
  - A_LESS_THAN_B: Constant indicating first argument is less than second
  - [SortSupport](../S/SortSupport.md): Type for sort support structure

- Called from (representative examples):
  - [btoidsortsupport](btoidsortsupport.md): Sets this as the comparison function for OID sort support

## Notes and Other Information
- This is a performance-optimized version of OID comparison for sort operations
- Uses direct Datum access rather than the PostgreSQL V1 function call convention
- Returns the same comparison semantics as btoidcmp but with lower overhead
- The SortSupport parameter is required by the interface but not used in this simple comparison
- Static linkage indicates this function is an internal implementation detail of the B-tree comparison system
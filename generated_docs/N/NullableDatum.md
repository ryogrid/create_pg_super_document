# NullableDatum

## Location
[src/include/postgres.h:72-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L72-L79)

## Overview
NullableDatum is a PostgreSQL structure that efficiently combines a Datum value with its nullness indicator in a single data structure to improve spatial locality and performance.

## Definition

```c
typedef struct NullableDatum
{
#define FIELDNO_NULLABLE_DATUM_DATUM 0
	Datum		value;
#define FIELDNO_NULLABLE_DATUM_ISNULL 1
	bool		isnull;
	/* due to alignment padding this could be used for flags for free */
} NullableDatum;
```
## Detailed Description
NullableDatum is a fundamental data structure in PostgreSQL designed to store both a Datum value and its null status in a single cohesive unit. This approach provides better spatial locality compared to storing datums and nullness indicators in separate arrays, which can improve cache performance and memory access patterns.

The structure is particularly useful in scenarios where PostgreSQL needs to handle potentially null values efficiently, such as during expression evaluation, function calls, and data processing operations. While the combined structure may consume slightly more memory due to alignment padding, the performance benefits from improved cache locality often outweigh this cost.

The design includes predefined field number constants (FIELDNO_NULLABLE_DATUM_DATUM and FIELDNO_NULLABLE_DATUM_ISNULL) that can be used for structured access to the fields, which is common in PostgreSQL's codebase for maintaining consistency and enabling potential optimizations.

## Parameters / Member Variables
- `value`: A Datum containing either a pass-by-value data type or a pointer to a pass-by-reference data type
- `isnull`: A boolean flag indicating whether the value is NULL (true) or contains valid data (false)
## Dependencies
- Functions called/Symbols referenced:
  - Datum (base type for the value field)
  - [bool](../b/bool.md) (standard boolean type)

- Used by (representative examples):
  - SerializedReindexState (catalog/index.c:110)
  - [AppendAttributeTuples](../A/AppendAttributeTuples.md) (catalog/index.c:510)
  - [index_create](../i/index_create.md) (catalog/index.c:738)
  - [ExecBuildAggTrans](../E/ExecBuildAggTrans.md) (executor/execExpr.c:3545)
  - [ExecInterpExpr](../E/ExecInterpExpr.md) (executor/execExprInterp.c:750, 1687)
  - [ExprEvalStep](../E/ExprEvalStep.md) (include/executor/execExpr.h:662)
  - [FunctionCallInfoBaseData](../F/FunctionCallInfoBaseData.md) (include/fmgr.h:95)
  - [JsonExprState](../J/JsonExprState.md) (include/nodes/execnodes.h:1028, 1031, 1044, 1047)

## Notes and Other Information
- The structure includes a comment noting that alignment padding could potentially be used for additional flags without increasing the overall structure size
- This design pattern reflects PostgreSQL's focus on performance optimization, particularly in memory access patterns
- The predefined FIELDNO_* constants suggest this structure is used in contexts where field-level access patterns are important
- Commonly used in executor and expression evaluation subsystems where null handling is critical
- The structure is defined in src/include/postgres.h (lines 72-79), making it available throughout the PostgreSQL codebase
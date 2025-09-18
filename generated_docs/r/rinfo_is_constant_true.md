# rinfo_is_constant_true

## Location
src/backend/optimizer/util/restrictinfo.c: 453 - 468

## Overview
A static inline helper function that detects whether a RestrictInfo's clause is a constant TRUE boolean expression.

## Definition
```c
static inline bool rinfo_is_constant_true(RestrictInfo *rinfo)
```

## Detailed Description
This function identifies RestrictInfo nodes that contain clauses representing constant TRUE values. While normal WHERE clause processing would eliminate such trivial conditions during canonicalization, the equivclass processing subsystem may generate RestrictInfo nodes with constant TRUE clauses for internal optimization reasons. These generated constant TRUE clauses need to be filtered out when creating the final execution plan to avoid unnecessary overhead.

The function performs a three-part check to ensure the clause is genuinely a constant TRUE: it must be a Const node, not NULL, and have a boolean value of TRUE.

## Parameters / Member Variables
- `rinfo`: Pointer to RestrictInfo structure containing the clause to be examined

## Dependencies
- Functions called/Symbols referenced:
  - IsA macro (PostgreSQL type checking)
  - Const struct (constisnull, constvalue fields)
  - [DatumGetBool](../D/DatumGetBool.md) macro (for extracting boolean value)
- Called from (representative examples):
  - [get_actual_clauses](../g/get_actual_clauses.md) (src/backend/optimizer/util/restrictinfo.c:479)
  - [extract_actual_clauses](../e/extract_actual_clauses.md) (src/backend/optimizer/util/restrictinfo.c:505)
  - [extract_actual_join_clauses](../e/extract_actual_join_clauses.md) (src/backend/optimizer/util/restrictinfo.c:539, 546)

## Notes and Other Information
- This is a static inline function, meaning it's only accessible within the restrictinfo.c file and is likely inlined by the compiler for performance
- The function is part of PostgreSQL's clause filtering mechanism that removes redundant constant TRUE conditions from execution plans
- Constant TRUE clauses can arise from equivclass processing, particularly when dealing with equivalence relationships between columns
- The three-condition check (IsA + !constisnull + DatumGetBool) ensures robust detection of genuine boolean TRUE constants while avoiding false positives from NULL values or non-boolean constants
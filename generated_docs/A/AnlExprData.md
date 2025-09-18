# AnlExprData

## Location
src/backend/statistics/extended_stats.c: 86 - 90

## Overview
AnlExprData is a lightweight structure that holds the essential information needed to analyze a single expression during extended statistics collection.

## Definition
```c
typedef struct AnlExprData
{
    Node       *expr;           /* expression to analyze */
    VacAttrStats *vacattrstat;  /* statistics attrs to analyze */
} AnlExprData;
```

## Detailed Description
AnlExprData serves as a container structure that pairs an expression with its corresponding statistics collection attributes during the extended statistics analysis phase. This structure is used internally within the extended statistics subsystem to organize and manage expression-based statistics collection.

The structure facilitates the analysis of complex expressions (not just simple column references) within extended statistics objects. It bridges the gap between the parsed expression representation and the statistics collection machinery, enabling PostgreSQL to collect meaningful statistics on computed values and expression results.

## Parameters / Member Variables
- `expr`: A pointer to the Node representing the expression to be analyzed. This can be any valid PostgreSQL expression tree node
- `vacattrstat`: A pointer to VacAttrStats structure containing the statistics collection attributes and configuration for analyzing this specific expression

## Dependencies
- Functions called/Symbols referenced:
  - Node (PostgreSQL parse tree node base type)
  - VacAttrStats (PostgreSQL statistics collection structure)

- Called from (representative examples):
  - BuildRelationExtStatistics
  - compute_expr_stats
  - expr_fetch_func
  - build_expr_data
  - serialize_expr_stats

## Notes and Other Information
This structure is defined in src/backend/statistics/extended_stats.c and is used exclusively during the expression statistics collection phase. It represents a pairing of an expression with its analysis parameters, enabling PostgreSQL to collect statistics on expression results rather than just simple column values.

The structure is typically used in arrays or lists when processing multiple expressions within a single extended statistics object. Each AnlExprData instance represents one expression that needs statistical analysis, and the vacattrstat member provides the framework for actually collecting and storing those statistics.

This design allows for efficient batch processing of expression statistics while maintaining clear separation between the expression definition and its statistical analysis parameters.
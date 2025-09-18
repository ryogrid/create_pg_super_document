# transformPartitionRangeBounds

## Location
src/backend/parser/parse_utilcmd.c: 4139 - 4255

## Overview
Converts raw grammar expressions for range partition bounds into validated PartitionRangeDatum structures, handling both finite values and infinite bounds (MINVALUE/MAXVALUE).

## Definition
```c
static List *transformPartitionRangeBounds(ParseState *pstate, List *blist, Relation parent)
```

## Detailed Description
This static function transforms a list of raw partition bound expressions into properly formatted PartitionRangeDatum structures for range partitioning. It processes each expression in the bound list, identifying special infinite range bounds ("minvalue" and "maxvalue") that come in as ColumnRef nodes, and transforming finite values through type checking and validation. The function handles both regular column references and expression-based partition keys.

For infinite bounds, the function creates PartitionRangeDatum nodes with appropriate kind markers (PARTITION_RANGE_DATUM_MINVALUE or PARTITION_RANGE_DATUM_MAXVALUE) and NULL values. For finite values, it transforms them using transformPartitionBoundValue and validates that they are not NULL. The function also ensures that once an infinite bound is encountered for one column, all subsequent columns must also use the same infinite bound type.

## Parameters / Member Variables
- `pstate`: ParseState providing context for error reporting and expression transformation
- `blist`: List of raw bound expressions from the parser to be transformed into range datums
- `parent`: The parent partitioned relation that defines the partitioning key structure

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - get_partition_exprs
  - makeNode
  - IsA
  - list_length
  - linitial
  - strVal
  - strcmp
  - [get_attname](../g/get_attname.md)
  - RelationGetRelid
  - [deparse_expression](../d/deparse_expression.md)
  - [list_nth](../l/list_nth.md)
  - [deparse_context_for](../d/deparse_context_for.md)
  - RelationGetRelationName
  - get_partition_col_typid
  - get_partition_col_typmod
  - get_partition_col_collation
  - [transformPartitionBoundValue](transformPartitionBoundValue.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [exprLocation](../e/exprLocation.md)
  - lappend
  - [validateInfiniteBounds](../v/validateInfiniteBounds.md)
- Called from (representative examples):
  - [transformPartitionBound](transformPartitionBound.md) (in src/backend/parser/parse_utilcmd.c:4121, 4124)

## Notes and Other Information
- Handles special keywords "minvalue" and "maxvalue" as infinite range bounds
- NULL values are explicitly rejected for range partition bounds
- The function maintains separate counters (i, j) for regular partition attributes and expression-based attributes
- Uses validateInfiniteBounds to ensure consistency when infinite bounds are used
- Each PartitionRangeDatum includes location information for error reporting
- For expression-based partitioning columns, generates readable column names using deparse_expression
- Returns an ordered list of PartitionRangeDatum structures corresponding to the input expressions
- Part of the range partitioning transformation pipeline in PostgreSQL's parser
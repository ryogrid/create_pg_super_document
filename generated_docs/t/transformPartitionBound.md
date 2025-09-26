# transformPartitionBound

## Location
[src/backend/parser/parse_utilcmd.c:3985-4138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L3985-L4138)

## Overview
Transforms and validates a partition bound specification according to the parent table's partitioning strategy (hash, list, or range).

## Definition
```c
PartitionBoundSpec *transformPartitionBound(ParseState *pstate, Relation parent, PartitionBoundSpec *spec)
```

## Detailed Description
This function processes partition bound specifications by validating them against the parent relation's partitioning strategy and transforming raw parse nodes into properly validated partition bounds. It handles three partitioning strategies: hash, list, and range partitioning. For hash partitioning, it validates modulus and remainder values. For list partitioning, it transforms individual list values and removes duplicates. For range partitioning, it transforms both lower and upper bound specifications. The function also handles default partitions, with special restrictions for hash partitioning.

The transformation process includes type checking, expression parsing, and ensuring that the partition bound specification matches the expected format for the given partitioning strategy. The function creates a copy of the input specification to avoid modifying the original parse tree.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and expression transformation context
- `parent`: The parent partitioned relation that defines the partitioning scheme
- `spec`: The raw partition bound specification from the parser to be transformed and validated

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [get_partition_strategy](../g/get_partition_strategy.md)
  - [get_partition_natts](../g/get_partition_natts.md)
  - [get_partition_exprs](../g/get_partition_exprs.md)
  - copyObject
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [parser_errposition](../p/parser_errposition.md)
  - [exprLocation](../e/exprLocation.md)
  - [get_attname](../g/get_attname.md)
  - [deparse_expression](../d/deparse_expression.md)
  - [deparse_context_for](../d/deparse_context_for.md)
  - [get_partition_col_typid](../g/get_partition_col_typid.md)
  - [get_partition_col_typmod](../g/get_partition_col_typmod.md)
  - [get_partition_col_collation](../g/get_partition_col_collation.md)
  - [transformPartitionBoundValue](transformPartitionBoundValue.md)
  - [transformPartitionRangeBounds](transformPartitionRangeBounds.md)
  - [equal](../e/equal.md)
  - [lappend](../l/lappend.md)
  - elog
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (in src/backend/commands/tablecmds.c:1108)
  - [transformPartitionCmd](transformPartitionCmd.md) (in src/backend/parser/parse_utilcmd.c:3942)

## Notes and Other Information
- [Hash](../H/Hash.md) partitioning does not support default partitions and will generate an error if attempted
- For list partitioning, duplicate values are automatically removed from the specification
- [Range](../R/Range.md) partitioning requires exact match between the number of bounds and partition key attributes
- The function preserves the original input by creating a copy using copyObject
- Validates that modulus values for hash partitioning are positive and remainder values are less than modulus
- For expression-based partitioning columns, uses deparse_expression to generate readable column names for error messages
- Returns a fully transformed PartitionBoundSpec ready for use by the execution system
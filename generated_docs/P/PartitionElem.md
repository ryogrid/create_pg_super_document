# PartitionElem

## Location
[src/include/nodes/parsenodes.h:860-868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L860-L868)

## Overview
PartitionElem represents a single partition key element in the parse tree, specifying either a column name or expression to be used for table partitioning along with its associated options.

## Definition


## Detailed Description
PartitionElem is a parse-time representation used in PostgreSQL's table partitioning system to define individual partition key components. It can represent either simple column-based partitioning (where name is specified and expr is NULL) or expression-based partitioning (where expr is specified and name is NULL). The structure encapsulates the partitioning element along with its collation and operator class specifications, providing the necessary information for the partitioning subsystem to properly distribute data across partitions.

## Parameters / Member Variables
- : NodeTag identifier for this node type
- : Name of the table column to partition on (NULL for expression-based partitioning)
- : Expression tree to partition on (NULL for simple column partitioning, can be raw or analyzed)
- : List specifying the collation to use for the partition key (NIL for default)
- : List specifying the desired operator class for the partition key (NIL for default)
- : Source location in the original SQL text (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (type for tracking source location)
- Called from (representative examples):
  - [transformPartitionSpec](../t/transformPartitionSpec.md) (processes partition specifications during table creation)
  - [ComputePartitionAttrs](../C/ComputePartitionAttrs.md) (computes partition key attributes)
  - [exprLocation](../e/exprLocation.md) (determines expression source location)

## Notes and Other Information
PartitionElem is fundamental to PostgreSQL's declarative partitioning feature introduced in version 10. Unlike indexes, partition keys are not stored on-disk in this format but are transformed into internal catalog representations. The structure supports both simple column partitioning and complex expression-based partitioning, enabling flexible partitioning strategies. The expr field can contain either raw expression trees from the parser or parse-analyzed expressions, providing flexibility in different processing stages.
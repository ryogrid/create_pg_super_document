# get_partition_operator

## Location
[src/backend/partitioning/partbounds.c:3832-3867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3832-L3867)

## Overview
Returns the OID of the operator for a given strategy and partition key column, determining if type relabeling is needed for binary compatibility.

## Definition
```c
static Oid get_partition_operator(PartitionKey key, int col, StrategyNumber strategy, bool *need_relabel)
```

## Detailed Description
This function retrieves the appropriate operator OID from the partitioning operator family for a specific column and strategy. It uses the operator class's declared input type for both left and right operand types when looking up the operator. The function also determines whether the partition key column requires a RelabelType node due to type differences between the partition key type and the operator class input type. This is crucial for ensuring type compatibility in partition constraint expressions.

## Parameters / Member Variables
- `key`: Pointer to PartitionKey structure containing partition metadata
- `col`: Column index within the partition key (0-based)
- `strategy`: Strategy number identifying the specific operator to retrieve (e.g., BTLessStrategyNumber)
- `need_relabel`: Output parameter set to true if RelabelType node is needed for type compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [get_opfamily_member](get_opfamily_member.md)
  - IsPolymorphicType
  - StrategyNumber
  - [PartitionKey](../P/PartitionKey.md)
- Called from (representative examples):
  - compare_range_bounds
  - [make_partition_op_expr](../m/make_partition_op_expr.md)

## Notes and Other Information
- This is a static function internal to partbounds.c
- Assumes partitioning key is of same type as partitioning opclass or at least binary-compatible
- Sets *need_relabel to true when opclass is not polymorphic and types differ, following parse_coerce.c conventions
- Throws ERROR if the required operator is not found in the partition operator family
- RECORDOID and polymorphic types are treated specially and don't require relabeling
- Essential for building correct partition constraint expressions with proper type handling
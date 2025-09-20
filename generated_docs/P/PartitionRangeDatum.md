# PartitionRangeDatum

## Location
[src/include/nodes/parsenodes.h:929-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L929-L938)

## Overview
PartitionRangeDatum represents one of the values in a range partition bound, which can be MINVALUE, MAXVALUE, or a specific bounded value.

## Definition

```c
typedef struct PartitionRangeDatum
{
	NodeTag		type;

	PartitionRangeDatumKind kind;
	Node	   *value;			/* Const (or A_Const in raw tree), if kind is
								 * PARTITION_RANGE_DATUM_VALUE, else NULL */

	ParseLoc	location;		/* token location, or -1 if unknown */
} PartitionRangeDatum;
```
## Detailed Description
PartitionRangeDatum is used in range partitioning to represent individual bound values that define partition boundaries. Each datum can represent either an unbounded value (MINVALUE for negative infinity, MAXVALUE for positive infinity) or a specific bounded value with an associated constant. These structures are used to build the lower and upper bounds lists in PartitionBoundSpec for range-partitioned tables.

Range partitions use these datums to define inclusive lower bounds and exclusive upper bounds. The unbounded values allow for open-ended ranges at the beginning and end of the partition range spectrum, while specific values define precise partition boundaries based on the partition key values.

## Parameters / Member Variables
- : Standard NodeTag for the PostgreSQL node system
- : Enumerated type indicating whether this datum represents MINVALUE, MAXVALUE, or a specific bounded value
- : Pointer to a Const node containing the actual value when kind is PARTITION_RANGE_DATUM_VALUE, otherwise NULL for unbounded values
- : Parse location in the original SQL text for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionRangeDatumKind](PartitionRangeDatumKind.md)
  - ParseLoc
  - NodeTag (inherited)
  - [Node](../N/Node.md) (for value storage)
- Called from (representative examples):
  - [transformPartitionRangeBounds](../t/transformPartitionRangeBounds.md)
  - compare_range_bounds
  - [check_new_partition_bound](../c/check_new_partition_bound.md)
  - [get_qual_for_range](../g/get_qual_for_range.md)
  - [make_one_partition_rbound](../m/make_one_partition_rbound.md)

## Notes and Other Information
- Used exclusively in range partitioning to define partition bounds
- MINVALUE and MAXVALUE provide unbounded range capabilities (negative and positive infinity)
- The value field is only meaningful when kind is PARTITION_RANGE_DATUM_VALUE
- These structures are created during partition bound parsing and used throughout the partition management system
- Critical for range partition pruning and constraint generation during query planning
- Multiple PartitionRangeDatum structures are combined to form complete range specifications in multi-column partition keys
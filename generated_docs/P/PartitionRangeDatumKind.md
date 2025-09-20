# PartitionRangeDatumKind

## Location
[src/include/nodes/parsenodes.h:927-928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L927-L928)

## Overview
PartitionRangeDatumKind is an enumeration that specifies the type of range partition boundary datum, distinguishing between explicit values, minimum bounds, and maximum bounds in PostgreSQL's range partitioning system.

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
PartitionRangeDatumKind defines the three types of boundary values that can appear in range partition definitions. It is essential for PostgreSQL's range partitioning functionality, allowing partitions to be bounded by explicit values or by infinite bounds (MINVALUE/MAXVALUE). The enum uses specific integer values (-1, 0, 1) that facilitate comparison operations during partition pruning and tuple routing. This classification system enables PostgreSQL to handle both finite partition boundaries and unbounded partitions that capture all values below or above a certain threshold.

## Parameters / Member Variables
- `PARTITION_RANGE_DATUM_MINVALUE`: Represents MINVALUE, a boundary less than any possible value (-1)
- `PARTITION_RANGE_DATUM_VALUE`: Represents a specific bounded value provided in the partition definition (0)
- `PARTITION_RANGE_DATUM_MAXVALUE`: Represents MAXVALUE, a boundary greater than any possible value (1)

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - [PartitionRangeDatum](PartitionRangeDatum.md) (in kind field)
  - PartitionBoundInfoData (in kind array field)
  - Various partitioning functions in partbounds.c

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:922-927
- Integer values are chosen to enable direct comparison: MINVALUE (-1) < VALUE (0) < MAXVALUE (1)
- Used extensively in range partition bound comparison and validation functions
- Essential for partition pruning optimization where the planner determines which partitions need to be scanned
- Only applies to range partitioned tables; not used for hash or list partitioning strategies
- Part of PartitionRangeDatum structure that represents individual boundary values in CREATE TABLE PARTITION OF statements
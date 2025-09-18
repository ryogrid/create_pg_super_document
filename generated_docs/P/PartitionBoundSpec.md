# PartitionBoundSpec

## Location
[src/include/partitioning/partdefs.h:20-21](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/partitioning/partdefs.h#L20-L21)

## Overview
A parse tree node that represents a partition bound specification, defining the portion of the partition key space assigned to a particular partition as specified in DDL commands.

## Definition


## Detailed Description
PartitionBoundSpec is a parse tree node structure that captures partition boundary specifications from SQL DDL statements (CREATE TABLE ... PARTITION OF). It represents the raw, parsed form of partition bounds before they are processed and stored in the system catalog (pg_class.relpartbound) or converted to runtime partition bound structures.

The structure is strategy-specific: for hash partitioning, it stores modulus and remainder values; for list partitioning, it contains a list of literal values; for range partitioning, it maintains separate lists for lower and upper bound datums. The structure also tracks whether this represents a default partition (which accepts values not handled by other partitions).

This is an intermediate representation used during DDL processing, existing between the raw parse tree (with A_Consts) and the final runtime partition boundary structures (PartitionBoundInfo).

## Parameters / Member Variables
- : NodeTag identifying this as a PartitionBoundSpec node
- : Partitioning strategy code (hash/list/range)
- : Boolean flag indicating if this is a default partition bound
- : For hash partitioning - the hash modulus value
- : For hash partitioning - the hash remainder value this partition accepts
- : For list partitioning - list of constant values this partition accepts
- : For range partitioning - list of lower boundary values (PartitionRangeDatums)
- : For range partitioning - list of upper boundary values (PartitionRangeDatums)
- : Source code location for error reporting (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree node type identifier)
  - [List](../L/List.md) (PostgreSQL list type)
  - ParseLoc (source location type)
  - [PartitionRangeDatum](PartitionRangeDatum.md) (range boundary datum structure)
  - PARTITION_STRATEGY constants

- Called from (representative examples):
  - [transformPartitionBound](../t/transformPartitionBound.md) (DDL statement processing)
  - [partition_bounds_create](../p/partition_bounds_create.md) (conversion to runtime bounds)
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md) (partition descriptor construction)
  - [StorePartitionBound](../S/StorePartitionBound.md) (catalog storage)
  - [get_qual_for_hash](../g/get_qual_for_hash.md)/get_qual_for_list/get_qual_for_range (constraint generation)

## Notes and Other Information
- Part of the SQL parse tree, not used during runtime tuple routing
- Stores partition bounds in their DDL-specified form before validation and conversion
- Supports all three partitioning strategies with appropriate strategy-specific fields
- Location information enables accurate error reporting during DDL processing
- Converted to PartitionBoundInfo structures for runtime use after validation
- Used by parser and DDL processing code, but not by executor or planner directly
- Serialized to pg_class.relpartbound for persistent storage of partition definitions
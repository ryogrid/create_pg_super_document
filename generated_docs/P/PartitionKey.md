# PartitionKey

## Location
src/include/partitioning/partdefs.h: 18 - 19

## Overview
A pointer to PartitionKeyData structure that contains comprehensive information about the partition key of a relation, including partitioning strategy, key attributes, operator information, and type details.

## Definition


## Detailed Description
PartitionKey is a pointer type to the PartitionKeyData structure that encapsulates all metadata necessary for partitioning operations on a table. It stores the partitioning strategy (hash, list, or range), identifies which columns or expressions form the partition key, and maintains operator and type information needed for partition boundary comparisons and tuple routing.

The structure supports both simple column-based partitioning and expression-based partitioning. For expression-based partitioning, the partition expressions are stored separately while maintaining null entries in the attribute number array. The structure also caches operator family information and function lookup data for efficient partition operations.

Type information is comprehensively stored for each partition key attribute, including type OIDs, type modifiers, length information, pass-by-value flags, alignment requirements, and collation data - all necessary for proper datum handling during partitioning operations.

## Parameters / Member Variables
(This is a typedef pointer, see PartitionKeyData for actual structure members)
- : Partitioning strategy (hash/list/range)
- : Number of partition key columns/expressions
- : Array of attribute numbers (0 for expressions)
- : List of partition expressions for expression-based partitioning
- : Operator families for each key attribute
- : Operator class input types
- : Function manager info for support functions
- : Collation for each key attribute
- : Complete type information

## Dependencies
- Functions called/Symbols referenced:
  - PartitionKeyData (underlying structure)
  - PartitionStrategy (partitioning strategy enum)
  - AttrNumber (attribute number type)
  - List (PostgreSQL list type)
  - Oid (object identifier type)
  - FmgrInfo (function manager info structure)

- Called from (representative examples):
  - RelationBuildPartitionKey (partition key construction)
  - get_partition_for_tuple (tuple routing to partitions)
  - CreatePartitionPruneState (partition pruning setup)
  - transformPartitionBound (DDL partition bound parsing)
  - partition_bounds_create (partition boundary creation)

## Notes and Other Information
- Cached in relation descriptor for efficient access during operations
- Supports hybrid partitioning schemes with both attributes and expressions
- Type information is pre-computed and cached to avoid repeated catalog lookups
- Function lookup information (partsupfunc) enables efficient comparisons during partition pruning
- Used extensively by both parser (DDL validation) and executor (tuple routing)
- Collation information ensures proper text comparison semantics for character data types
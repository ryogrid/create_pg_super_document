# get_partition_col_typid

## Location
src/include/utils/partcache.h: 86 - 91

## Overview
Returns the data type OID for a specific partition key column in a partitioned table.

## Definition
```c
static inline Oid get_partition_col_typid(PartitionKey key, int col)
```

## Detailed Description
This inline function provides access to the data type OIDs of partition key columns stored in a PartitionKey structure. The type OID identifies the PostgreSQL data type of the column used for partitioning at the specified partition key position. This information is crucial for:
- Type checking during partition bound validation
- Proper comparison operations during tuple routing  
- Ensuring partition bounds are compatible with column types
- Building appropriate comparison operators for partition constraints

The function returns the PostgreSQL system catalog OID that uniquely identifies the data type, which can be used to look up type-specific information such as input/output functions, comparison operators, and type modifiers.

## Parameters / Member Variables
- `key`: A PartitionKey structure containing partitioning metadata for a partitioned table
- `col`: The partition key column index (0-based) to retrieve the type OID for

## Dependencies
- Functions called/Symbols referenced:
  - PartitionKey (struct type)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - ExecBuildSlotPartitionKeyDescription
  - transformPartitionBound
  - transformPartitionRangeBounds

## Notes and Other Information
- This is a static inline function defined in partcache.h for efficient access
- The function returns the parttypid[col] field from the PartitionKey structure
- Returns an Oid which references entries in PostgreSQL system catalogs
- Used for type safety and proper operator selection in partition operations
- Essential for validating partition bound values against column types
- Located at src/include/utils/partcache.h:85-89
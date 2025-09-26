# PartitionSchemeData

## Location
[src/include/nodes/pathnodes.h:582-596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L582-L596)

## Overview
PartitionSchemeData is a structure that stores the general properties of a partition method, including strategy, partitioning attributes, and cached type information for efficient partition comparison operations.

## Definition
```c
typedef struct PartitionSchemeData
{
    char        strategy;       /* partition strategy */
    int16       partnatts;      /* number of partition attributes */
    Oid        *partopfamily;   /* OIDs of operator families */
    Oid        *partopcintype;  /* OIDs of opclass declared input data types */
    Oid        *partcollation;  /* OIDs of partitioning collations */

    /* Cached information about partition key data types. */
    int16      *parttyplen;
    bool       *parttypbyval;

    /* Cached information about partition comparison functions. */
    struct FmgrInfo *partsupfunc;
} PartitionSchemeData;
```

## Detailed Description
PartitionSchemeData represents a partition scheme that can be shared across multiple relations partitioned in the same way. The structure incorporates only the general properties of the partition method (LIST vs. RANGE, number of partitioning columns and type information) rather than specific partition bounds. This design allows multiple partitioned relations with identical partitioning schemes to share the same PartitionScheme object, which is stored in a list attached to the PlannerInfo structure.

The structure stores opclass-declared input data types instead of partition key datatypes, as the former are used for partition bound comparisons. These types are expected to be binary compatible with partition key data types, ensuring consistent byval and length properties.

## Parameters / Member Variables
- `strategy`: Character indicating the partition strategy (e.g., LIST or RANGE partitioning)
- `partnatts`: Number of partition attributes/columns used for partitioning
- `partopfamily`: Array of OIDs representing the operator families for each partition attribute
- `partopcintype`: Array of OIDs for opclass-declared input data types used in partition comparisons
- `partcollation`: Array of OIDs representing collations used for each partition attribute
- `parttyplen`: Cached array of type lengths for partition key data types
- `parttypbyval`: Cached array of boolean values indicating whether partition key types are passed by value
- `partsupfunc`: Cached array of function manager info structures for partition comparison functions

## Dependencies
- Functions called/Symbols referenced:
  - struct FmgrInfo (for partsupfunc member)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [find_partition_scheme](../f/find_partition_scheme.md) (src/backend/optimizer/util/plancat.c:2513)
  - [PartitionScheme](PartitionScheme.md) (src/include/nodes/pathnodes.h:598)

## Notes and Other Information
- Multiple relations with identical partitioning schemes share the same PartitionScheme object for memory efficiency
- The structure focuses on partitioning metadata rather than actual partition bounds
- Caches type and function information to optimize partition-related operations
- Part of the PostgreSQL query planner infrastructure for handling partitioned tables
- Located in pathnodes.h as part of the planner node definitions
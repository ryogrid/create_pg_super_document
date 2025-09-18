# generate_partition_qual

## Location
src/backend/utils/cache/partcache.c: 337 - 432

## Overview
Recursively generates the complete partition constraint qualification for a partition by combining its own partition bounds with inherited constraints from parent partitions up the hierarchy.

## Definition
```c
static List *generate_partition_qual(Relation rel)
```

## Detailed Description
generate_partition_qual is the core function responsible for constructing partition constraint qualifications. It performs a recursive traversal up the partition hierarchy, collecting and combining partition bounds at each level to build the complete set of constraints that define which rows belong to a specific partition.

The function implements sophisticated caching and memory management:
- Results are cached in rel->rd_partcheck with rd_partcheckvalid flag
- Creates a dedicated memory context ("partition constraint") for cached results
- Uses the caller's context for working data to avoid memory leaks

Key operations include:
- Retrieving partition bounds from pg_class.relpartbound
- Converting bounds to qualification expressions via get_qual_from_partbound
- Recursively collecting parent partition constraints
- Mapping variable attribute numbers to match the target partition's schema
- Caching results for future access

The recursive nature handles complex partition hierarchies where a partition can itself be partitioned (sub-partitioning), ensuring all inherited constraints are properly combined.

## Parameters / Member Variables
- `rel`: The partition relation for which to generate constraints

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (guards against infinite recursion)
  - copyObject (creates copies for caching and return)
  - get_partition_parent (finds parent relation OID)
  - relation_open (opens parent relation)
  - SysCacheGetAttr (retrieves partition bounds from pg_class)
  - stringToNode/TextDatumGetCString (parses bound specification)
  - get_qual_from_partbound (converts bounds to constraint expressions)
  - list_concat (combines parent and local constraints)
  - map_partition_varattnos (adjusts variable references)
  - AllocSetContextCreate (creates cache memory context)
- Called from:
  - RelationGetPartitionQual (primary entry point)
  - get_partition_qual_relid (OID-based entry point)
  - generate_partition_qual (recursive self-call for parent constraints)

## Notes and Other Information
- Function is static (internal to partcache.c)
- Implements recursive descent through partition hierarchy
- Handles both direct partitions and sub-partitioned tables
- Results are cached with rd_partcheckvalid flag to avoid recomputation
- Uses check_stack_depth() to prevent stack overflow in deep hierarchies
- Memory management creates dedicated context only when results are non-NIL
- Maintains parent relation lock until commit for consistency
- Returns working copy to caller while caching separate copy in relcache
- Variable attribute number mapping ensures constraints reference correct columns in target partition
- Handles the case where partition bounds may be NULL (no constraints)
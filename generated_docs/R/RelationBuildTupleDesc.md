# RelationBuildTupleDesc

## Location
src/backend/utils/cache/relcache.c: 521 - 732

## Overview
RelationBuildTupleDesc constructs the complete tuple descriptor for a relation by scanning pg_attribute and incorporating constraint, default, and missing value information.

## Definition
static void RelationBuildTupleDesc(Relation relation)

## Detailed Description
RelationBuildTupleDesc is responsible for building the comprehensive tuple descriptor (rd_att) for a relation by scanning the pg_attribute system catalog and gathering all attribute-related metadata. The function reads attribute definitions, processes constraints (NOT NULL, generated columns), default values, and missing values to create a complete TupleDesc structure. It handles memory management carefully by allocating structures in CacheMemoryContext and implements optimizations such as setting the first attribute's cache offset to zero.

The function performs a systematic scan of pg_attribute filtering for user attributes (attnum > 0), copies attribute data into the tuple descriptor, and sets up constraint information including defaults and check constraints. It also handles the special case of missing values for columns added with DEFAULT clauses.

## Parameters / Member Variables
- : The relation descriptor whose tuple descriptor (rd_att) will be populated with attribute information

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md), TupleConstr, AttrMissing (data structure types)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (memory allocation in cache context)
  - BTGreaterStrategyNumber, Int16GetDatum (scan key construction)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (system catalog scanning)
  - RelationGetNumberOfAttributes (attribute count retrieval)
  - ATTRIBUTE_FIXED_PART_SIZE, ATTRIBUTE_GENERATED_STORED (constants)
  - [heap_getattr](../h/heap_getattr.md) (attribute value extraction from tuple)
  - [array_get_element](../a/array_get_element.md), datumCopy (missing value processing)
  - [AttrDefaultFetch](../A/AttrDefaultFetch.md), CheckConstraintFetch (constraint processing)
- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md) (during complete relation descriptor construction)

## Notes and Other Information
- Scans only user attributes (attnum > 0) for efficiency using index optimization
- Sets up tuple type information including tdtypeid and tdtypmod fields
- Handles missing values for columns added with ALTER TABLE ADD COLUMN DEFAULT
- Implements cache offset optimization by setting first attribute's attcacheoff to 0
- Validates attribute numbering and reports errors for invalid attribute numbers
- Processes constraint information including NOT NULL, generated columns, defaults, and check constraints
- Uses memory context switching to ensure all allocated data lives in CacheMemoryContext
- Includes assertion checking for attcacheoff values in debug builds
- Critical component of relcache initialization providing complete attribute metadata
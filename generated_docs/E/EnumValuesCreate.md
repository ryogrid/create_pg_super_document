# EnumValuesCreate

## Location
src/backend/catalog/pg_enum.c: 84 - 223

## Overview
Creates entries in the pg_enum catalog table for each supplied enum value during CREATE TYPE AS ENUM, assigning sorted OIDs and managing transaction-level enum type tracking.

## Definition


## Detailed Description
EnumValuesCreate is the core function responsible for populating the pg_enum catalog with enum value entries during enum type creation. The function implements several critical PostgreSQL enum management features:

1. **Transaction Tracking**: Records the enum type OID in uncommitted_enum_types hash table if called at transaction level 1, enabling proper handling of subsequent ALTER ADD VALUE operations.

2. **OID Assignment Strategy**: Allocates even-numbered OIDs to enum values to enable direct OID comparison in enum comparison functions, avoiding the need for catalog lookups during comparisons.

3. **Batch Processing**: Uses multi-insert optimization to efficiently insert multiple enum values in batches, improving performance for enums with many values.

4. **Sort Order Management**: Assigns enumsortorder values sequentially (1, 2, 3...) to maintain proper enum value ordering.

The function assumes it will be called even for empty enum types, making it the single entry point for enum type transaction management.

## Parameters / Member Variables
- : The OID of the enum type being created
- : List of String values representing the enum labels to be created

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - init_uncommitted_enum_types
  - hash_search
  - GetNewOidWithIndex
  - qsort
  - CatalogOpenIndexes
  - CatalogTuplesMultiInsertWithInfo
  - MakeSingleTupleTableSlot
  - ExecClearTuple
  - ExecStoreVirtualTuple
  - ExecDropSingleTupleTableSlot
- Called from:
  - DefineEnum (src/backend/commands/typecmds.c:1221)

## Notes and Other Information
- The function deliberately does not check for duplicate values in the input list, relying on unique index violations to catch duplicates
- Even-numbered OID assignment is a performance optimization that allows enum comparison functions to compare OIDs directly without catalog lookups
- The uncommitted_enum_types tracking only occurs at transaction level 1, not in subtransactions, to optimize for the most common usage patterns
- Multi-insert batching is limited by MAX_CATALOG_MULTI_INSERT_BYTES to control memory usage
- Enum labels are stored in NAME fields and are subject to NAMEDATALEN length restrictions
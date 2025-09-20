# AppendAttributeTuples

## Location
[src/backend/catalog/index.c:510-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L510-L560)

## Overview
Inserts attribute information from an index's tuple descriptor into the pg_attribute system catalog, making the index columns visible to the PostgreSQL system catalogs.

## Definition

```c
static void
AppendAttributeTuples(Relation indexRelation, const Datum *attopts, const NullableDatum *stattargets)
```
## Detailed Description
This function completes the index creation process by inserting attribute metadata into the pg_attribute system catalog. It takes the tuple descriptor from the newly created index relation and converts each attribute into a pg_attribute tuple that gets stored in the system catalog. The function handles optional attribute-specific data including storage options (attopts) and statistics targets (stattargets). It opens the pg_attribute relation with proper locking, prepares the catalog indexes for insertion, and uses InsertPgAttributeTuples to perform the actual insertion. This step is crucial because it makes the index attributes visible to PostgreSQL's metadata queries and ensures the index is properly integrated into the system catalog structure.

## Parameters / Member Variables
- : Relation pointer to the index whose attributes will be inserted into pg_attribute
- : Array of Datum values containing attribute options (storage parameters), may be NULL
- : Array of NullableDatum values containing statistics targets for each attribute, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_array: Allocates zero-initialized array for extra attribute data
  - [FormExtraData_pg_attribute](../F/FormExtraData_pg_attribute.md): Structure type for additional attribute metadata
  - [NullableDatum](../N/NullableDatum.md): Structure for nullable Datum values
  - CatalogIndexState: Structure for managing catalog index state
  - table_open: Opens the pg_attribute system catalog with specified lock mode
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md): Opens indexes associated with the pg_attribute catalog
  - RelationGetDescr: Retrieves the tuple descriptor from the index relation
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md): Performs the actual insertion of attribute tuples
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md): Closes catalog indexes and updates them with new entries
  - table_close: Closes the pg_attribute catalog relation
- Called from (representative examples):
  - index_create: During normal index creation process
  - SerializedReindexState: During reindex operations

## Notes and Other Information
- This is a static function, only used within the same source file
- Uses RowExclusiveLock on pg_attribute to prevent concurrent modifications during insertion
- Properly handles optional parameters by checking for NULL values and setting appropriate isnull flags
- Memory allocation for attrs_extra is done using palloc0_array for zero initialization
- The function maintains proper catalog consistency by opening/closing indexes during the insertion
- InvalidOid is passed to InsertPgAttributeTuples indicating this is for an index (not a table)
- Essential for making index attributes visible in system views like information_schema and pg_attribute
- Part of the final phase of index creation after physical storage structures are established
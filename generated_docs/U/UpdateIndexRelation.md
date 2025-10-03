# UpdateIndexRelation

## Location
[src/backend/catalog/index.c:561-723](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L561-L723)

## Overview
UpdateIndexRelation is a static function that constructs and inserts a new entry in the pg_index system catalog to record metadata about an index relation.

## Definition

```c
static void
UpdateIndexRelation(Oid indexoid,
					Oid heapoid,
					Oid parentIndexId,
					const IndexInfo *indexInfo,
					const Oid *collationOids,
					const Oid *opclassOids,
					const int16 *coloptions,
					bool primary,
					bool isexclusion,
					bool immediate,
					bool isvalid,
					bool isready)
```
## Detailed Description
This function creates a complete entry in the pg_index system catalog with all necessary metadata for an index. It processes the provided index information and transforms it into the appropriate format for storage in the catalog. The function handles conversion of index expressions and predicates to text format, builds various vector types for storing index keys and options, and performs the actual catalog insertion.

The function is responsible for setting all index flags and properties including uniqueness, primary key status, exclusion constraints, validity, and readiness states. It serves as the central point for recording index metadata during index creation operations.

## Parameters / Member Variables
- `indexoid`: Object identifier of the index relation being created
- `heapoid`: Object identifier of the table (heap) that this index belongs to
- `parentIndexId`: Object identifier of parent index (for partitioned indexes)
- `*indexInfo`: IndexInfo structure containing index attribute numbers, expressions, predicates, and other properties
- `*collationOids`: Array of collation object identifiers for each index key column
- `*opclassOids`: Array of operator class object identifiers for each index key column
- `*coloptions`: Array of option flags for each index key column
- `primary`: Boolean flag indicating if this is a primary key index
- `isexclusion`: Boolean flag indicating if this is an exclusion constraint index
- `immediate`: Boolean flag indicating if constraint checking is immediate
- `isvalid`: Boolean flag indicating if the index is valid for queries
- `isready`: Boolean flag indicating if the index is ready for inserts
## Dependencies
- Functions called/Symbols referenced:
  - [buildint2vector](../b/buildint2vector.md) (for index keys and options)
  - [buildoidvector](../b/buildoidvector.md) (for collations and operator classes)
  - [nodeToString](../n/nodeToString.md) (for expressions and predicates)
  - [make_ands_explicit](../m/make_ands_explicit.md) (for predicate normalization)
  - [heap_form_tuple](../h/heap_form_tuple.md) (for tuple construction)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md) (for catalog insertion)
  - [heap_freetuple](../h/heap_freetuple.md) (for memory cleanup)
- Called from (representative examples):
  - [index_create](../i/index_create.md)

## Notes and Other Information
- This is a static function internal to index.c, not exposed in the public API
- The function properly handles NULL values for optional fields like expressions and predicates
- All boolean flags are set to appropriate defaults (e.g., indisclustered=false, indislive=true)
- The function uses RowExclusiveLock when accessing the pg_index catalog
- Memory management is handled properly with pfree() calls for temporary strings
- The function is located at src/backend/catalog/index.c:561-723

## Simplified Source

```c
static void UpdateIndexRelation(Oid indexoid, Oid heapoid, Oid parentIndexId,
                               const IndexInfo *indexInfo, const Oid *collationOids,
                               const Oid *opclassOids, const int16 *coloptions,
                               bool primary, bool isexclusion, bool immediate,
                               bool isvalid, bool isready)
{
    int2vector *indkey;
    oidvector *indcollation, *indclass;
    int2vector *indoption;
    Datum exprsDatum, predDatum;
    Datum values[Natts_pg_index];
    bool nulls[Natts_pg_index] = {0};
    Relation pg_index;
    HeapTuple tuple;

    // Build vectors for index keys, collations, opclasses, and options
    indkey = buildint2vector(NULL, indexInfo->ii_NumIndexAttrs);
    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
        indkey->values[i] = indexInfo->ii_IndexAttrNumbers[i];

    indcollation = buildoidvector(collationOids, indexInfo->ii_NumIndexKeyAttrs);
    indclass = buildoidvector(opclassOids, indexInfo->ii_NumIndexKeyAttrs);
    indoption = buildint2vector(coloptions, indexInfo->ii_NumIndexKeyAttrs);

    // Convert expressions and predicates to text format
    if (indexInfo->ii_Expressions != NIL)
    {
        char *exprsString = nodeToString(indexInfo->ii_Expressions);
        exprsDatum = CStringGetTextDatum(exprsString);
        pfree(exprsString);
    }
    else
        exprsDatum = (Datum) 0;

    if (indexInfo->ii_Predicate != NIL)
    {
        char *predString = nodeToString(make_ands_explicit(indexInfo->ii_Predicate));
        predDatum = CStringGetTextDatum(predString);
        pfree(predString);
    }
    else
        predDatum = (Datum) 0;

    // Open pg_index catalog
    pg_index = table_open(IndexRelationId, RowExclusiveLock);

    // Fill in the values array for pg_index tuple
    values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid);
    values[Anum_pg_index_indrelid - 1] = ObjectIdGetDatum(heapoid);
    values[Anum_pg_index_indnatts - 1] = Int16GetDatum(indexInfo->ii_NumIndexAttrs);
    values[Anum_pg_index_indisunique - 1] = BoolGetDatum(indexInfo->ii_Unique);
    values[Anum_pg_index_indisprimary - 1] = BoolGetDatum(primary);
    values[Anum_pg_index_indisexclusion - 1] = BoolGetDatum(isexclusion);
    values[Anum_pg_index_indisvalid - 1] = BoolGetDatum(isvalid);
    values[Anum_pg_index_indisready - 1] = BoolGetDatum(isready);
    // ... set other boolean flags and vector fields ...

    // Create and insert the tuple
    tuple = heap_form_tuple(RelationGetDescr(pg_index), values, nulls);
    CatalogTupleInsert(pg_index, tuple);

    // Cleanup
    table_close(pg_index, RowExclusiveLock);
    heap_freetuple(tuple);
}
```
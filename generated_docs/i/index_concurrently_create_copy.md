# index_concurrently_create_copy

## Location
[src/backend/catalog/index.c:1298-1481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L1298-L1481)

## Overview
index_concurrently_create_copy creates a concurrent copy of an existing index based on its definition, used primarily during concurrent reindex operations.

## Definition

```c
Oid
index_concurrently_create_copy(Relation heapRelation, Oid oldIndexId,
							   Oid tablespaceOid, const char *newName)
```
## Detailed Description
This function creates a new index that is a copy of an existing index, intended for use during concurrent reindex operations. It extracts all necessary metadata from the original index including column definitions, operator classes, collations, expressions, predicates, and options. The new index is created with INDEX_CREATE_SKIP_BUILD and INDEX_CREATE_CONCURRENT flags, meaning the catalog entries are created but the actual index data building is deferred to a later phase.

The function performs thorough metadata extraction from system catalogs (pg_index, pg_class, pg_attribute) rather than relying solely on the IndexInfo structure, as some information like expressions and predicates may have been flattened for planner use. It explicitly prevents creation of indexes with exclusion constraints during concurrent operations.

## Parameters / Member Variables
- `heapRelation`: The table relation that the index belongs to
- `oldIndexId`: Object identifier of the existing index to copy
- `tablespaceOid`: Tablespace where the new index should be created
- `*newName`: Name for the new index copy
## Dependencies
- Functions called/Symbols referenced:
  - [index_open](index_open.md) (to access the original index)
  - [BuildIndexInfo](../B/BuildIndexInfo.md) (to extract index metadata)
  - [SearchSysCache1](../S/SearchSysCache1.md)/SearchSysCache2 (for catalog lookups)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)/SysCacheGetAttr (for attribute retrieval)
  - TextDatumGetCString (for text field conversion)
  - [stringToNode](../s/stringToNode.md) (for parsing stored expressions and predicates)
  - [makeIndexInfo](../m/makeIndexInfo.md) (to construct new IndexInfo structure)
  - [get_attoptions](../g/get_attoptions.md) (for attribute-specific options)
  - [make_ands_implicit](../m/make_ands_implicit.md) (for predicate format conversion)
  - [index_create](index_create.md) (to create the new index)
  - [index_close](index_close.md) (for cleanup)
- Called from (representative examples):
  - Concurrent reindex operations

## Notes and Other Information
- Returns the OID of the newly created index copy
- The new index is created but not built (INDEX_CREATE_SKIP_BUILD flag)
- Explicitly rejects indexes with exclusion constraints for concurrent creation
- Extracts complete metadata including expressions, predicates, and per-attribute options
- Creates the index with CONCURRENT flag for proper concurrent reindex handling
- Performs extensive catalog lookups to ensure complete metadata transfer
- The function is specifically designed for the concurrent reindex workflow
- Located at src/backend/catalog/index.c:1298-1481

## Simplified Source

```c
Oid index_concurrently_create_copy(Relation heapRelation, Oid oldIndexId,
                                   Oid tablespaceOid, const char *newName) {
    Relation indexRelation;
    IndexInfo *oldInfo, *newInfo;
    Oid newIndexId = InvalidOid;
    HeapTuple indexTuple, classTuple;
    List *indexColNames = NIL;
    List *indexExprs = NIL;
    List *indexPreds = NIL;

    // Open the old index to copy its definition
    indexRelation = index_open(oldIndexId, RowExclusiveLock);
    oldInfo = BuildIndexInfo(indexRelation);

    // Concurrent build not supported for exclusion constraints
    if (oldInfo->ii_ExclusionOps != NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("concurrent index creation for exclusion constraints is not supported")));
    }

    // Get index metadata from system catalogs
    indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(oldIndexId));
    if (!HeapTupleIsValid(indexTuple)) {
        elog(ERROR, "cache lookup failed for index %u", oldIndexId);
    }

    // Extract operator classes and column options
    Datum indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, indexTuple,
                                                 Anum_pg_index_indclass);
    oidvector *indclass = (oidvector *) DatumGetPointer(indclassDatum);

    Datum colOptionDatum = SysCacheGetAttrNotNull(INDEXRELID, indexTuple,
                                                  Anum_pg_index_indoption);
    int2vector *indcoloptions = (int2vector *) DatumGetPointer(colOptionDatum);

    // Get relation options
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(oldIndexId));
    if (!HeapTupleIsValid(classTuple)) {
        elog(ERROR, "cache lookup failed for relation %u", oldIndexId);
    }
    bool isnull;
    Datum reloptionsDatum = SysCacheGetAttr(RELOID, classTuple,
                                           Anum_pg_class_reloptions, &isnull);

    // Extract expressions and predicates from catalogs
    if (oldInfo->ii_Expressions != NIL) {
        Datum exprDatum = SysCacheGetAttrNotNull(INDEXRELID, indexTuple,
                                                 Anum_pg_index_indexprs);
        char *exprString = TextDatumGetCString(exprDatum);
        indexExprs = (List *) stringToNode(exprString);
        pfree(exprString);
    }

    if (oldInfo->ii_Predicate != NIL) {
        Datum predDatum = SysCacheGetAttrNotNull(INDEXRELID, indexTuple,
                                                 Anum_pg_index_indpred);
        char *predString = TextDatumGetCString(predDatum);
        indexPreds = (List *) stringToNode(predString);
        indexPreds = make_ands_implicit((Expr *) indexPreds);
        pfree(predString);
    }

    // Create new IndexInfo structure
    newInfo = makeIndexInfo(oldInfo->ii_NumIndexAttrs,
                           oldInfo->ii_NumIndexKeyAttrs,
                           oldInfo->ii_Am,
                           indexExprs,
                           indexPreds,
                           oldInfo->ii_Unique,
                           oldInfo->ii_NullsNotDistinct,
                           false,  // not ready for inserts
                           true,
                           indexRelation->rd_indam->amsummarizing);

    // Extract column names and attribute numbers
    for (int i = 0; i < oldInfo->ii_NumIndexAttrs; i++) {
        TupleDesc indexTupDesc = RelationGetDescr(indexRelation);
        Form_pg_attribute att = TupleDescAttr(indexTupDesc, i);
        indexColNames = lappend(indexColNames, NameStr(att->attname));
        newInfo->ii_IndexAttrNumbers[i] = oldInfo->ii_IndexAttrNumbers[i];
    }

    // Extract per-attribute options and statistics targets
    Datum *opclassOptions = palloc0(sizeof(Datum) * newInfo->ii_NumIndexAttrs);
    NullableDatum *stattargets = palloc0_array(NullableDatum, newInfo->ii_NumIndexAttrs);

    for (int i = 0; i < newInfo->ii_NumIndexAttrs; i++) {
        opclassOptions[i] = get_attoptions(oldIndexId, i + 1);

        HeapTuple tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(oldIndexId),
                                      Int16GetDatum(i + 1));
        if (!HeapTupleIsValid(tp)) {
            elog(ERROR, "cache lookup failed for attribute %d of relation %u",
                 i + 1, oldIndexId);
        }
        Datum dat = SysCacheGetAttr(ATTNUM, tp, Anum_pg_attribute_attstattarget,
                                   &isnull);
        ReleaseSysCache(tp);
        stattargets[i].value = dat;
        stattargets[i].isnull = isnull;
    }

    // Create the new index (catalog entries only, no build)
    newIndexId = index_create(heapRelation, newName, InvalidOid, InvalidOid,
                             InvalidOid, InvalidRelFileNumber, newInfo,
                             indexColNames, indexRelation->rd_rel->relam,
                             tablespaceOid, indexRelation->rd_indcollation,
                             indclass->values, opclassOptions,
                             indcoloptions->values, stattargets,
                             reloptionsDatum,
                             INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_CONCURRENT,
                             0, true, false, NULL);

    // Clean up
    index_close(indexRelation, NoLock);
    ReleaseSysCache(indexTuple);
    ReleaseSysCache(classTuple);

    return newIndexId;
}
```
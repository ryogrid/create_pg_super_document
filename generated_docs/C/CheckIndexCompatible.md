# CheckIndexCompatible

## Location
[src/backend/commands/indexcmds.c:177-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L177-L359)

## Overview
Determines whether an existing index definition is compatible with a prospective index definition, such that the existing index storage could become the storage of the new index, avoiding a rebuild.

## Definition
```c
bool CheckIndexCompatible(Oid oldId, const char *accessMethodName, const List *attributeList, const List *exclusionOpNames)
```

## Detailed Description
CheckIndexCompatible is tailored to the needs of ALTER TABLE ALTER TYPE operations, which recreate indexes that depend on a changing column from their pg_get_indexdef or pg_get_constraintdef definitions. The function performs compatibility checks by comparing operator classes, collations, and exclusion operators between the old and new index definitions.

Most column type changes that can skip a table rewrite do not invalidate indexes. The function acknowledges this when all operator classes, collations and exclusion operators match. It omits some sanity checks of DefineIndex and assumes that the old and new indexes have the same number of columns and that if one has an expression column or predicate, both do.

The function performs several key compatibility checks:
- Verifies access method compatibility
- Compares operator classes and collations
- Handles polymorphic operators by checking actual input types
- Validates exclusion constraint operators if present
- Checks opclass options for compatibility

Currently, the function does not implement tests to verify compatibility of expression columns or predicates, so it assumes any such index is incompatible.

## Parameters / Member Variables
- `oldId`: The OID of the existing index to check compatibility against
- `accessMethodName`: Name of the access method to use for the new index
- `attributeList`: A list of IndexElem specifying columns and expressions to index on
- `exclusionOpNames`: List of names of exclusion-constraint operators, or NIL if not an exclusion constraint

## Dependencies
- Functions called/Symbols referenced:
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md)
  - [makeIndexInfo](../m/makeIndexInfo.md)
  - [ComputeIndexAttrs](ComputeIndexAttrs.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [get_opclass_input_type](../g/get_opclass_input_type.md)
  - IsPolymorphicType
  - [CompareOpclassOptions](CompareOpclassOptions.md)
  - [RelationGetExclusionInfo](../R/RelationGetExclusionInfo.md)
  - [op_input_types](../o/op_input_types.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [TryReuseIndex](../T/TryReuseIndex.md)

## Notes and Other Information
- Returns false immediately if the index has expressions, predicates, or is invalid
- For polymorphic operator class input types, column type changes break compatibility
- Changes in operator class options also break compatibility
- The function assumes compatibility issues are primarily related to operator classes, collations, and exclusion operators
- Used primarily in ALTER TABLE operations to determine if an index rebuild can be avoided
- Located in src/backend/commands/indexcmds.c:177-359

## Simplified Source

```c
bool CheckIndexCompatible(Oid oldId, const char *accessMethodName,
                         const List *attributeList, const List *exclusionOpNames) {
    // Get relation info and validate access method
    Oid relationId = IndexGetRelation(oldId, false);
    HeapTuple tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("access method \"%s\" does not exist", accessMethodName)));

    Form_pg_am accessMethodForm = (Form_pg_am) GETSTRUCT(tuple);
    Oid accessMethodId = accessMethodForm->oid;
    IndexAmRoutine *amRoutine = GetIndexAmRoutine(accessMethodForm->amhandler);
    ReleaseSysCache(tuple);

    // Compute new index attributes
    int numberOfAttributes = list_length(attributeList);
    IndexInfo *indexInfo = makeIndexInfo(numberOfAttributes, numberOfAttributes,
                                        accessMethodId, NIL, NIL, false, false,
                                        false, false, amRoutine->amsummarizing);

    Oid *typeIds = palloc_array(Oid, numberOfAttributes);
    Oid *collationIds = palloc_array(Oid, numberOfAttributes);
    Oid *opclassIds = palloc_array(Oid, numberOfAttributes);
    Datum *opclassOptions = palloc_array(Datum, numberOfAttributes);
    int16 *coloptions = palloc_array(int16, numberOfAttributes);

    ComputeIndexAttrs(indexInfo, typeIds, collationIds, opclassIds, opclassOptions,
                     coloptions, attributeList, exclusionOpNames, relationId,
                     accessMethodName, accessMethodId, amRoutine->amcanorder,
                     false, InvalidOid, 0, NULL);

    // Get existing index info
    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(oldId));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for index %u", oldId);

    Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(tuple);

    // Check for expressions, predicates, or invalid index
    if (!(heap_attisnull(tuple, Anum_pg_index_indpred, NULL) &&
          heap_attisnull(tuple, Anum_pg_index_indexprs, NULL) &&
          indexForm->indisvalid)) {
        ReleaseSysCache(tuple);
        return false;
    }

    // Compare operator classes and collations
    int old_natts = indexForm->indnkeyatts;
    Datum d = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indcollation);
    oidvector *old_indcollation = (oidvector *) DatumGetPointer(d);
    d = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indclass);
    oidvector *old_indclass = (oidvector *) DatumGetPointer(d);

    bool ret = (memcmp(old_indclass->values, opclassIds, old_natts * sizeof(Oid)) == 0 &&
               memcmp(old_indcollation->values, collationIds, old_natts * sizeof(Oid)) == 0);

    ReleaseSysCache(tuple);

    if (!ret)
        return false;

    // Check polymorphic type compatibility
    Relation irel = index_open(oldId, AccessShareLock);
    for (int i = 0; i < old_natts; i++) {
        if (IsPolymorphicType(get_opclass_input_type(opclassIds[i])) &&
            TupleDescAttr(irel->rd_att, i)->atttypid != typeIds[i]) {
            ret = false;
            break;
        }
    }

    // Check opclass options compatibility
    if (ret) {
        Datum *oldOpclassOptions = palloc_array(Datum, old_natts);
        for (int i = 0; i < old_natts; i++)
            oldOpclassOptions[i] = get_attoptions(oldId, i + 1);
        ret = CompareOpclassOptions(oldOpclassOptions, opclassOptions, old_natts);
        pfree(oldOpclassOptions);
    }

    // Check exclusion operators if present
    if (ret && indexInfo->ii_ExclusionOps != NULL) {
        Oid *old_operators, *old_procs;
        uint16 *old_strats;
        RelationGetExclusionInfo(irel, &old_operators, &old_procs, &old_strats);
        ret = memcmp(old_operators, indexInfo->ii_ExclusionOps, old_natts * sizeof(Oid)) == 0;

        // Check polymorphic exclusion operators
        if (ret) {
            for (int i = 0; i < old_natts && ret; i++) {
                Oid left, right;
                op_input_types(indexInfo->ii_ExclusionOps[i], &left, &right);
                if ((IsPolymorphicType(left) || IsPolymorphicType(right)) &&
                    TupleDescAttr(irel->rd_att, i)->atttypid != typeIds[i]) {
                    ret = false;
                }
            }
        }
    }

    index_close(irel, NoLock);
    return ret;
}
```
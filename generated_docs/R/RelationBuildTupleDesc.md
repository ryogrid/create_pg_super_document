# RelationBuildTupleDesc

## Location
[src/backend/utils/cache/relcache.c:521-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L521-L732)

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

## Simplified Source

```c
static void RelationBuildTupleDesc(Relation relation) {
    // Initialize tuple descriptor type information
    relation->rd_att->tdtypeid = relation->rd_rel->reltype ?
        relation->rd_rel->reltype : RECORDOID;
    relation->rd_att->tdtypmod = -1;

    // Initialize constraint structure
    TupleConstr *constr = MemoryContextAllocZero(CacheMemoryContext,
                                                sizeof(TupleConstr));
    constr->has_not_null = false;
    constr->has_generated_stored = false;

    // Set up scan keys to find only user attributes (attnum > 0)
    ScanKeyData skey[2];
    ScanKeyInit(&skey[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    ScanKeyInit(&skey[1], Anum_pg_attribute_attnum, BTGreaterStrategyNumber,
                F_INT2GT, Int16GetDatum(0));

    // Scan pg_attribute catalog for this relation's attributes
    Relation pg_attribute_desc = table_open(AttributeRelationId, AccessShareLock);
    SysScanDesc pg_attribute_scan = systable_beginscan(pg_attribute_desc,
                                                      AttributeRelidNumIndexId,
                                                      criticalRelcachesBuilt,
                                                      NULL, 2, skey);

    // Process each attribute tuple
    int need = RelationGetNumberOfAttributes(relation);
    int ndef = 0;
    AttrMissing *attrmiss = NULL;
    HeapTuple pg_attribute_tuple;

    while (HeapTupleIsValid(pg_attribute_tuple = systable_getnext(pg_attribute_scan))) {
        Form_pg_attribute attp = (Form_pg_attribute) GETSTRUCT(pg_attribute_tuple);
        int attnum = attp->attnum;

        // Validate attribute number
        if (attnum <= 0 || attnum > RelationGetNumberOfAttributes(relation))
            elog(ERROR, "invalid attribute number %d for relation \"%s\"",
                 attp->attnum, RelationGetRelationName(relation));

        // Copy attribute data into tuple descriptor
        memcpy(TupleDescAttr(relation->rd_att, attnum - 1), attp,
               ATTRIBUTE_FIXED_PART_SIZE);

        // Track constraint information
        if (attp->attnotnull)
            constr->has_not_null = true;
        if (attp->attgenerated == ATTRIBUTE_GENERATED_STORED)
            constr->has_generated_stored = true;
        if (attp->atthasdef)
            ndef++;

        // Handle missing values for ALTER TABLE ADD COLUMN DEFAULT
        if (attp->atthasmissing) {
            // Extract missing value from pg_attribute tuple
            Datum missingval = heap_getattr(pg_attribute_tuple,
                                          Anum_pg_attribute_attmissingval,
                                          pg_attribute_desc->rd_att, &missingNull);
            if (!missingNull) {
                // Allocate missing values array if needed
                if (attrmiss == NULL)
                    attrmiss = MemoryContextAllocZero(CacheMemoryContext,
                                                     relation->rd_rel->relnatts *
                                                     sizeof(AttrMissing));

                // Extract and copy the missing value
                Datum missval = array_get_element(missingval, 1, &one, -1,
                                                attp->attlen, attp->attbyval,
                                                attp->attalign, &is_null);

                if (attp->attbyval) {
                    attrmiss[attnum - 1].am_value = missval;
                } else {
                    // Copy value in cache memory context
                    MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
                    attrmiss[attnum - 1].am_value = datumCopy(missval,
                                                            attp->attbyval,
                                                            attp->attlen);
                    MemoryContextSwitchTo(oldcxt);
                }
                attrmiss[attnum - 1].am_present = true;
            }
        }

        if (--need == 0) break;  // Found all expected attributes
    }

    // Clean up scan
    systable_endscan(pg_attribute_scan);
    table_close(pg_attribute_desc, AccessShareLock);

    // Verify we found all expected attributes
    if (need != 0)
        elog(ERROR, "pg_attribute catalog is missing %d attribute(s) for relation OID %u",
             need, RelationGetRelid(relation));

    // Optimize: set first attribute cache offset to 0
    if (RelationGetNumberOfAttributes(relation) > 0)
        TupleDescAttr(relation->rd_att, 0)->attcacheoff = 0;

    // Set up constraint information if any constraints exist
    if (constr->has_not_null || constr->has_generated_stored ||
        ndef > 0 || attrmiss || relation->rd_rel->relchecks > 0) {

        relation->rd_att->constr = constr;

        // Fetch default values if any exist
        if (ndef > 0)
            AttrDefaultFetch(relation, ndef);
        else
            constr->num_defval = 0;

        constr->missing = attrmiss;

        // Fetch check constraints if any exist
        if (relation->rd_rel->relchecks > 0)
            CheckConstraintFetch(relation);
        else
            constr->num_check = 0;
    } else {
        // No constraints, free the constraint structure
        pfree(constr);
        relation->rd_att->constr = NULL;
    }
}
```
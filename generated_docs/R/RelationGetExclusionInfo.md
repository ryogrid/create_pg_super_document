# RelationGetExclusionInfo

## Location
[src/backend/utils/cache/relcache.c:5596-5727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5596-L5727)

## Overview
Retrieves detailed information about an exclusion constraint associated with an index, including the exclusion operators, their underlying function OIDs, and strategy numbers.

## Definition

```c
void
RelationGetExclusionInfo(Relation indexRelation,
						 Oid **operators,
						 Oid **procs,
						 uint16 **strategies)
```
## Detailed Description
This function extracts and caches information about an exclusion constraint that is associated with the given index relation. It should only be called for indexes that are known to have an associated exclusion constraint.

The function returns three arrays (allocated in the caller's memory context):
1. **operators**: Array of exclusion operator OIDs used in the constraint
2. **procs**: Array of underlying function OIDs for those operators  
3. **strategies**: Array of strategy numbers for the operators in the index's operator classes

The function first checks if the information is already cached in the index relation's cache entry (, , ). If cached, it simply copies the data and returns.

If not cached, it searches the  system catalog to find the exclusion constraint record associated with the index. It uses the constraint's  (parent relation OID) and scans for  type constraints with matching .

Once found, it extracts the operator OIDs from the  array field, then looks up the corresponding function OIDs and strategy numbers. Finally, it caches all this information in the index relation's cache context for future use.

## Parameters / Member Variables
- `indexRelation`: The index relation that has an exclusion constraint
- `**operators`: Output parameter - pointer to array of exclusion operator OIDs
- `**procs`: Output parameter - pointer to array of underlying function OIDs
- `**strategies`: Output parameter - pointer to array of strategy numbers
## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [palloc](../p/palloc.md)/memcpy
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)/table_close
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan/systable_getnext
  - [fastgetattr](../f/fastgetattr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS/ARR_NDIM/ARR_HASNULL/ARR_ELEMTYPE/ARR_DATA_PTR
  - [get_opcode](../g/get_opcode.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
- Called from (representative examples):
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)

## Notes and Other Information
- Should only be called for indexes with known exclusion constraints
- Results are cached in the index relation's cache context () for performance
- The function performs validation on the  array to ensure it's a well-formed 1-D OID array
- Strategy number lookup should not fail since operators are validated at index creation time
- Uses  when scanning the constraint catalog
- The returned arrays are allocated in the caller's memory context and should be freed when no longer needed

## Simplified Source

```c
void RelationGetExclusionInfo(Relation indexRelation,
                             Oid **operators,
                             Oid **procs,
                             uint16 **strategies) {
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(indexRelation);

    // Allocate result arrays in caller's context
    *operators = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
    *procs = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
    *strategies = (uint16 *) palloc(sizeof(uint16) * indnkeyatts);

    // Return cached data if available
    if (indexRelation->rd_exclstrats != NULL) {
        memcpy(*operators, indexRelation->rd_exclops, sizeof(Oid) * indnkeyatts);
        memcpy(*procs, indexRelation->rd_exclprocs, sizeof(Oid) * indnkeyatts);
        memcpy(*strategies, indexRelation->rd_exclstrats, sizeof(uint16) * indnkeyatts);
        return;
    }

    // Search pg_constraint for the exclusion constraint
    setup_constraint_scan_key(indexRelation);
    conrel = table_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 1, skey);

    // Find the matching exclusion constraint
    while ((htup = systable_getnext(conscan)) != NULL) {
        Form_pg_constraint conform = (Form_pg_constraint) GETSTRUCT(htup);

        if (conform->contype == CONSTRAINT_EXCLUSION &&
            conform->conindid == RelationGetRelid(indexRelation)) {

            // Extract operator OIDs from conexclop array
            extract_operator_oids_from_constraint(htup, *operators, indnkeyatts);
            break;
        }
    }

    systable_endscan(conscan);
    table_close(conrel, AccessShareLock);

    // Look up function OIDs and strategy numbers for each operator
    for (int i = 0; i < indnkeyatts; i++) {
        (*procs)[i] = get_opcode((*operators)[i]);
        (*strategies)[i] = get_op_opfamily_strategy((*operators)[i],
                                                   indexRelation->rd_opfamily[i]);
    }

    // Cache the results in the relation's context
    cache_exclusion_info_in_relation(indexRelation, *operators, *procs, *strategies, indnkeyatts);
}
```
# MergeWithExistingConstraint

## Location
[src/backend/catalog/heap.c:2557-2711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2557-L2711)

## Overview
MergeWithExistingConstraint checks for pre-existing check constraints with the same name and either merges them with appropriate inheritance settings or reports conflicts as needed.

## Definition

```c
static bool
MergeWithExistingConstraint(Relation rel, const char *ccname, Node *expr,
							bool allow_merge, bool is_local,
							bool is_initially_valid,
							bool is_no_inherit)
```
## Detailed Description
MergeWithExistingConstraint is a static function that handles constraint merging during constraint addition operations. The function searches for existing constraints with the same name and relation, validates that they are identical check constraints, and either merges them by updating inheritance metadata or reports appropriate conflicts.

The function performs comprehensive conflict detection and resolution:
1. Searches pg_constraint for existing constraints with the same name and relation
2. Validates that any found constraint is a check constraint with identical expression
3. Handles special cases for partition relations and inheritance scenarios
4. Updates constraint inheritance counters and local status when merging is allowed
5. Reports various types of conflicts (duplicate names, inheritance mismatches, validation conflicts)

The merging logic handles complex inheritance scenarios, including special handling for partitioned tables where inheritance constraints have different semantics.

## Parameters / Member Variables
- `rel`: The relation for which constraint merging is being attempted
- `*ccname`: The name of the constraint to check for conflicts
- `*expr`: The constraint expression to compare against existing constraints
- `allow_merge`: Whether merging with existing constraints is permitted
- `is_local`: Whether the new constraint is being defined locally
- `is_initially_valid`: Whether the new constraint is initially valid
- `is_no_inherit`: Whether the new constraint should not be inherited
## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - CONSTRAINT_CHECK
  - [fastgetattr](../f/fastgetattr.md)
  - [equal](../e/equal.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - ERRCODE_DUPLICATE_OBJECT
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md)

## Notes and Other Information
- Returns true if constraint was merged (is a duplicate), false if it has a unique name, or throws error on conflicts
- Special handling for partitioned tables where inherited constraints are always non-local
- Prevents changing inherited constraints to "no inherit" status to maintain inheritance propagation
- Cannot merge constraints where child is "no inherit" or has validation mismatches
- Updates inheritance count (coninhcount) and local status (conislocal) when merging
- Issues NOTICE message when successfully merging constraints
- Validates against various constraint property conflicts including inheritance and validation status
- Related to MergeConstraintsIntoExisting function (mentioned in comments)

## Simplified Source

```c
static bool MergeWithExistingConstraint(Relation rel, const char *ccname, Node *expr,
                                       bool allow_merge, bool is_local,
                                       bool is_initially_valid, bool is_no_inherit) {
    bool found = false;
    Relation conDesc;
    SysScanDesc conscan;
    ScanKeyData skey[3];
    HeapTuple tup;

    // Search for existing constraint with same name and relation
    conDesc = table_open(ConstraintRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(ccname));

    conscan = systable_beginscan(conDesc, ConstraintRelidTypidNameIndexId,
                                 true, NULL, 3, skey);

    // Check if constraint already exists
    if (HeapTupleIsValid(tup = systable_getnext(conscan))) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);

        // Verify it's a check constraint with identical expression
        if (con->contype == CONSTRAINT_CHECK) {
            Datum val;
            bool isnull;

            val = fastgetattr(tup, Anum_pg_constraint_conbin,
                             conDesc->rd_att, &isnull);
            if (isnull) {
                elog(ERROR, "null conbin for rel %s",
                     RelationGetRelationName(rel));
            }
            if (equal(expr, stringToNode(TextDatumGetCString(val)))) {
                found = true;
            }
        }

        // Allow merging if constraint is purely inherited and this is local
        if (is_local && !con->conislocal && !rel->rd_rel->relispartition) {
            allow_merge = true;
        }

        // Error if not found or merging not allowed
        if (!found || !allow_merge) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("constraint \"%s\" for relation \"%s\" already exists",
                                  ccname, RelationGetRelationName(rel))));
        }

        // Validate merge compatibility
        if (con->connoinherit) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errmsg("constraint \"%s\" conflicts with non-inherited constraint on relation \"%s\"",
                                  ccname, RelationGetRelationName(rel))));
        }

        if (con->coninhcount > 0 && is_no_inherit) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errmsg("constraint \"%s\" conflicts with inherited constraint on relation \"%s\"",
                                  ccname, RelationGetRelationName(rel))));
        }

        if (is_initially_valid && !con->convalidated) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errmsg("constraint \"%s\" conflicts with NOT VALID constraint on relation \"%s\"",
                                  ccname, RelationGetRelationName(rel))));
        }

        // Merge the constraints by updating inheritance metadata
        ereport(NOTICE, (errmsg("merging constraint \"%s\" with inherited definition",
                               ccname)));

        tup = heap_copytuple(tup);
        con = (Form_pg_constraint) GETSTRUCT(tup);

        // Handle partition vs regular table inheritance differently
        if (rel->rd_rel->relispartition) {
            con->coninhcount = 1;
            con->conislocal = false;
        } else {
            if (is_local) {
                con->conislocal = true;
            } else {
                con->coninhcount++;
            }

            if (con->coninhcount < 0) {
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                               errmsg("too many inheritance parents")));
            }
        }

        if (is_no_inherit) {
            Assert(is_local);
            con->connoinherit = true;
        }

        CatalogTupleUpdate(conDesc, &tup->t_self, tup);
    }

    systable_endscan(conscan);
    table_close(conDesc, RowExclusiveLock);

    return found;
}
```
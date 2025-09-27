# ExecConstraints

## Location
[src/backend/executor/execMain.c:1918-2052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1918-L2052)

## Overview
Validates traditional NOT NULL and check constraints for a tuple, handling tuple format conversion for partitioned tables but excluding partition constraints.

## Definition
```c
void ExecConstraints(ResultRelInfo *resultRelInfo,
                    TupleTableSlot *slot, EState *estate)
```

## Detailed Description
ExecConstraints is the primary function for enforcing traditional table constraints (NOT NULL and check constraints) during tuple insertion and updates. The function operates in two phases: first validating NOT NULL constraints by iterating through all attributes with NOT NULL requirements, then evaluating check constraints using ExecRelCheck. For partitioned tables, it handles tuple format conversion by mapping partition-specific tuple formats back to root table formats to ensure error messages accurately reflect the original input data. The function explicitly excludes partition constraints, which are handled separately by ExecPartitionCheck.

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo structure for the target relation, containing constraint metadata and potential root relation reference
- `slot`: TupleTableSlot containing the tuple to be validated against table constraints  
- `estate`: Execution state providing access to column modification tracking and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [slot_attisnull](../s/slot_attisnull.md)
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [MakeTupleTableSlot](../M/MakeTupleTableSlot.md)
  - [ExecGetInsertedCols](ExecGetInsertedCols.md)
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md)
  - [bms_union](../b/bms_union.md)
  - [ExecBuildSlotValueDescription](ExecBuildSlotValueDescription.md)
  - [ExecRelCheck](ExecRelCheck.md)
  - [errtablecol](../e/errtablecol.md)
  - [errtableconstraint](../e/errtableconstraint.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md)
  - [ExecSimpleRelationUpdate](ExecSimpleRelationUpdate.md)
  - [ExecInsert](ExecInsert.md)
  - [ExecUpdateAct](ExecUpdateAct.md)

## Notes and Other Information
- Explicitly excludes partition constraint validation, which is handled by ExecPartitionCheck
- Performs tuple format conversion for routed tuples to ensure error messages match original input format
- Validates NOT NULL constraints by iterating through all table attributes with NOT NULL requirements
- Check constraints are evaluated using ExecRelCheck, which returns the name of the first failed constraint
- Generates comprehensive error messages including tuple value descriptions limited to 64 characters
- Uses appropriate error codes: ERRCODE_NOT_NULL_VIOLATION for NOT NULL violations and ERRCODE_CHECK_VIOLATION for check constraint failures
- Handles both root tables and partitioned table scenarios with appropriate attribute mapping

## Simplified Source

```c
// Simplified version of ExecConstraints
void ExecConstraints(ResultRelInfo *resultRelInfo,
                    TupleTableSlot *slot, EState *estate) {
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    TupleConstr *constr = tupdesc->constr;
    Bitmapset *modifiedCols;

    Assert(constr);  // Should only be called when constraints exist

    // Phase 1: Check NOT NULL constraints
    if (constr->has_not_null) {
        int natts = tupdesc->natts;

        for (int attrChk = 1; attrChk <= natts; attrChk++) {
            Form_pg_attribute att = TupleDescAttr(tupdesc, attrChk - 1);

            // Check if attribute has NOT NULL constraint and value is null
            if (att->attnotnull && slot_attisnull(slot, attrChk)) {
                Relation orig_rel = rel;

                // Handle tuple format conversion for partitioned tables
                if (resultRelInfo->ri_RootResultRelInfo) {
                    // Convert partition tuple back to root table format for error reporting
                    ResultRelInfo *rootrel = resultRelInfo->ri_RootResultRelInfo;
                    AttrMap *map = build_attribute_mapping(tupdesc, rootrel);

                    if (map != NULL) {
                        slot = convert_slot_format(map, slot, rootrel);
                    }

                    modifiedCols = get_modified_columns(rootrel, estate);
                    rel = rootrel->ri_RelationDesc;
                } else {
                    modifiedCols = get_modified_columns(resultRelInfo, estate);
                }

                // Build error description and report NOT NULL violation
                char *val_desc = ExecBuildSlotValueDescription(rel, slot, tupdesc,
                                                             modifiedCols, 64);

                ereport(ERROR,
                       (errcode(ERRCODE_NOT_NULL_VIOLATION),
                        errmsg("null value in column \"%s\" violates not-null constraint",
                               NameStr(att->attname)),
                        val_desc ? errdetail("Failing row contains %s.", val_desc) : 0));
            }
        }
    }

    // Phase 2: Check user-defined check constraints
    if (rel->rd_rel->relchecks > 0) {
        const char *failed_constraint = ExecRelCheck(resultRelInfo, slot, estate);

        if (failed_constraint != NULL) {
            Relation orig_rel = rel;

            // Handle tuple format conversion for partitioned tables (same as above)
            if (resultRelInfo->ri_RootResultRelInfo) {
                ResultRelInfo *rootrel = resultRelInfo->ri_RootResultRelInfo;
                AttrMap *map = build_attribute_mapping(tupdesc, rootrel);

                if (map != NULL) {
                    slot = convert_slot_format(map, slot, rootrel);
                }

                modifiedCols = get_modified_columns(rootrel, estate);
                rel = rootrel->ri_RelationDesc;
            } else {
                modifiedCols = get_modified_columns(resultRelInfo, estate);
            }

            // Build error description and report check constraint violation
            char *val_desc = ExecBuildSlotValueDescription(rel, slot, tupdesc,
                                                         modifiedCols, 64);

            ereport(ERROR,
                   (errcode(ERRCODE_CHECK_VIOLATION),
                    errmsg("new row violates check constraint \"%s\"", failed_constraint),
                    val_desc ? errdetail("Failing row contains %s.", val_desc) : 0));
        }
    }
}
```

Key simplifications made:
- Consolidated duplicate tuple format conversion code into conceptual helper functions
- Removed detailed low-level attribute mapping operations for clarity
- Simplified variable declarations and combined related operations
- Abstracted complex tuple format conversion with descriptive function names
- Maintained the two-phase validation structure (NOT NULL, then check constraints)
- Preserved essential error reporting with appropriate error codes
- Focused on the main execution path while noting partition handling complexity
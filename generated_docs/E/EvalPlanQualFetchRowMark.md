# EvalPlanQualFetchRowMark

## Location
[src/backend/executor/execMain.c:2628-2738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2628-L2738)

## Overview
EvalPlanQualFetchRowMark fetches the current row value for a non-locked relation during EPQ operations, handling different row mark types and relation kinds including foreign tables.

## Definition

```c
bool
EvalPlanQualFetchRowMark(EPQState *epqstate, Index rti, TupleTableSlot *slot)
```
## Detailed Description
This function is a core component of PostgreSQL's EPQ (Eval Plan Qual) mechanism that retrieves tuple data for relations that need to be rescanned during concurrent update detection. It handles two main row mark types: ROW_MARK_REFERENCE (which fetches tuples by their ctid) and ROW_MARK_COPY (which uses stored whole-row values). For child relations in inheritance hierarchies, it validates that the current row actually belongs to the expected relation by checking the tableoid. The function includes special handling for foreign tables by delegating to the appropriate FDW (Foreign Data Wrapper) routine. It returns true if a substitution tuple was successfully found and false otherwise.

## Parameters / Member Variables
- : Pointer to the EPQState containing row mark information and the original slot
- : Range table index (1-based) identifying the specific relation
- : TupleTableSlot where the fetched tuple data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - RowMarkRequiresRowShareLock
  - [ExecGetJunkAttribute](ExecGetJunkAttribute.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - [ExecStoreHeapTupleDatum](ExecStoreHeapTupleDatum.md)
  - TupIsNull
- Called from (representative examples):
  - [ExecScanFetch](ExecScanFetch.md)
  - EvalPlanQualSetSlot

## Notes and Other Information
- Does not support locking row marks and will error if encountered
- Handles inheritance hierarchies by checking tableoid to ensure correct child relation
- For foreign tables, delegates to FDW RefetchForeignRow routine
- For ROW_MARK_REFERENCE: fetches tuple by ctid using table_tuple_fetch_row_version
- For ROW_MARK_COPY: reconstructs tuple from stored whole-row datum
- Returns false for NULL values which can occur for relations on the inside of outer joins
- Uses SnapshotAny for tuple fetching to get the most recent version
- Part of PostgreSQL's MVCC concurrency control infrastructure

## Simplified Source

```c
bool
EvalPlanQualFetchRowMark(EPQState *epqstate, Index rti, TupleTableSlot *slot)
{
    ExecAuxRowMark *earm = epqstate->relsubs_rowmark[rti - 1];
    ExecRowMark *erm = earm->rowmark;
    Datum datum;
    bool isNull;

    Assert(earm != NULL);
    Assert(epqstate->origslot != NULL);

    // Error if trying to use locking row marks
    if (RowMarkRequiresRowShareLock(erm->markType))
        elog(ERROR, "EvalPlanQual doesn't support locking rowmarks");

    // For child relations, verify this row belongs to the correct relation
    if (erm->rti != erm->prti)
    {
        datum = ExecGetJunkAttribute(epqstate->origslot, earm->toidAttNo, &isNull);
        if (isNull)
            return false;  // Null tableoid (e.g., outer join)

        Oid tableoid = DatumGetObjectId(datum);
        if (tableoid != erm->relid)
            return false;  // Wrong child relation
    }

    if (erm->markType == ROW_MARK_REFERENCE)
    {
        // Fetch tuple by ctid
        datum = ExecGetJunkAttribute(epqstate->origslot, earm->ctidAttNo, &isNull);
        if (isNull)
            return false;

        if (erm->relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        {
            // Foreign table: delegate to FDW
            FdwRoutine *fdwroutine = GetFdwRoutineForRelation(erm->relation, false);
            if (fdwroutine->RefetchForeignRow == NULL)
                ereport(ERROR, "cannot lock rows in foreign table");

            bool updated = false;
            fdwroutine->RefetchForeignRow(epqstate->recheckestate, erm, datum, slot, &updated);

            if (TupIsNull(slot))
                elog(ERROR, "failed to fetch tuple for EvalPlanQual recheck");
            return true;
        }
        else
        {
            // Regular table: fetch by ctid
            if (!table_tuple_fetch_row_version(erm->relation,
                                              (ItemPointer) DatumGetPointer(datum),
                                              SnapshotAny, slot))
                elog(ERROR, "failed to fetch tuple for EvalPlanQual recheck");
            return true;
        }
    }
    else
    {
        // ROW_MARK_COPY: use stored whole-row value
        Assert(erm->markType == ROW_MARK_COPY);

        datum = ExecGetJunkAttribute(epqstate->origslot, earm->wholeAttNo, &isNull);
        if (isNull)
            return false;

        ExecStoreHeapTupleDatum(datum, slot);
        return true;
    }
}
```
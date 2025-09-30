# ExecEvalFieldSelect

## Location
[src/backend/executor/execExprInterp.c:3173-3297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3173-L3297)

## Overview
Evaluates a FieldSelect node by extracting a specific field from a composite/record type, handling both expanded records and standard heap tuples with type validation and error checking.

## Definition
```c
void ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
ExecEvalFieldSelect implements field selection from composite types (records/tuples) in PostgreSQL's expression evaluation system. It handles the extraction of individual fields from composite values, supporting both optimized expanded record representations and standard heap tuple formats.

The function includes comprehensive validation including type compatibility checks (important after ALTER COLUMN TYPE operations), dropped column handling, and boundary checking for field numbers. It provides optimized access paths for expanded records while maintaining compatibility with standard tuple representations.

The implementation includes special handling for NULL records (returns NULL), dropped columns (returns NULL), and type mismatches (throws errors). Field numbering follows PostgreSQL conventions where user columns start at 1, and system columns are not supported.

## Parameters / Member Variables
- `state`: ExprState containing the overall expression evaluation context (currently unused)
- `econtext`: ExprContext providing runtime evaluation context (currently unused)  
- `op`: ExprEvalStep containing the specific operation data including:
  - `op->d.fieldselect.fieldnum`: AttrNumber indicating which field to extract (1-based)
  - `op->d.fieldselect.resulttype`: Expected OID of the result type for validation
  - `op->d.fieldselect.rowcache`: Cache for tuple descriptor lookups
  - `op->resvalue`: Pointer to the input record and storage for result value
  - `op->resnull`: Pointer to NULL flags for input and result

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED: Macro to detect expanded record format
  - [DatumGetEOHP](../D/DatumGetEOHP.md): Extracts expanded object header pointer
  - [expanded_record_get_tupdesc](../e/expanded_record_get_tupdesc.md): Gets tuple descriptor from expanded record
  - [expanded_record_get_field](../e/expanded_record_get_field.md): Extracts field from expanded record
  - DatumGetHeapTupleHeader: Converts Datum to HeapTupleHeader
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple header
  - [get_cached_rowtype](../g/get_cached_rowtype.md): Retrieves cached tuple descriptor for type
  - HeapTupleHeaderGetDatumLength: Gets tuple length for HeapTuple construction
  - [heap_getattr](../h/heap_getattr.md): Extracts attribute from standard heap tuple
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation context

## Notes and Other Information
- Supports both expanded records (optimized format) and standard heap tuples
- Includes type safety checking to detect schema changes after ALTER COLUMN TYPE
- Does not support system columns (negative field numbers) for security and consistency
- Uses caching mechanisms for tuple descriptor lookups to improve performance
- Part of PostgreSQL's compiled expression evaluation system
- Dropped columns are handled gracefully by returning NULL rather than errors
- Field numbering is 1-based following PostgreSQL attribute numbering conventions

## Simplified Source

```c
void ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    AttrNumber fieldnum = op->d.fieldselect.fieldnum;
    Datum tupDatum;
    TupleDesc tupDesc;
    Form_pg_attribute attr;

    // NULL record -> NULL result
    if (*op->resnull)
        return;

    tupDatum = *op->resvalue;

    // Fast path for expanded records
    if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(tupDatum))) {
        ExpandedRecordHeader *erh = (ExpandedRecordHeader *) DatumGetEOHP(tupDatum);
        tupDesc = expanded_record_get_tupdesc(erh);

        // Validate field number and get attribute info
        if (fieldnum <= 0 || fieldnum > tupDesc->natts)
            elog(ERROR, "invalid field number %d", fieldnum);

        attr = TupleDescAttr(tupDesc, fieldnum - 1);

        // Handle dropped columns and type mismatches
        if (attr->attisdropped) {
            *op->resnull = true;
            return;
        }

        if (op->d.fieldselect.resulttype != attr->atttypid)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("attribute %d has wrong type", fieldnum)));

        // Extract field from expanded record
        *op->resvalue = expanded_record_get_field(erh, fieldnum, op->resnull);
    } else {
        // Standard heap tuple path
        HeapTupleHeader tuple = DatumGetHeapTupleHeader(tupDatum);
        HeapTupleData tmptup;

        // Get tuple descriptor and validate field
        tupDesc = get_cached_rowtype(HeapTupleHeaderGetTypeId(tuple),
                                   HeapTupleHeaderGetTypMod(tuple),
                                   &op->d.fieldselect.rowcache, NULL);

        if (fieldnum <= 0 || fieldnum > tupDesc->natts)
            elog(ERROR, "invalid field number %d", fieldnum);

        attr = TupleDescAttr(tupDesc, fieldnum - 1);

        // Handle dropped columns and type mismatches
        if (attr->attisdropped) {
            *op->resnull = true;
            return;
        }

        if (op->d.fieldselect.resulttype != attr->atttypid)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("attribute %d has wrong type", fieldnum)));

        // Extract field from heap tuple
        tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
        tmptup.t_data = tuple;
        *op->resvalue = heap_getattr(&tmptup, fieldnum, tupDesc, op->resnull);
    }
}
```
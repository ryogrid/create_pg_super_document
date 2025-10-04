# plperl_return_next_internal

## Location
[src/pl/plperl/plperl.c:3275-3403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3275-L3403)

## Overview
Internal function that handles the core logic of return_next functionality for PL/Perl SETOF functions, managing tuple storage and memory context.

## Definition
```c
static void plperl_return_next_internal(SV *sv)
```

## Detailed Description
This function implements the internal logic for PL/Perl's return_next functionality, which allows set-returning functions to yield one result at a time. It handles both composite and scalar return types, managing the creation and population of a tuple store that accumulates results across multiple return_next calls within a single function invocation.

The function performs several key operations:
1. Validates that the function is declared as SETOF
2. On first call, determines the output tuple type and creates a tuple store
3. Manages memory contexts to prevent memory leaks during repeated calls
4. Converts Perl values to PostgreSQL tuples and stores them in the tuple store
5. Handles both composite types (hash references) and scalar types

## Parameters / Member Variables
- `sv`: Perl scalar value (SV*) to be returned as the next result tuple. Can be NULL, a hash reference for composite types, or a scalar for simple types.

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - AllocSetContextCreate
  - [plperl_build_tuple_result](plperl_build_tuple_result.md)
  - [domain_check](../d/domain_check.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - [tuplestore_puttuple](../t/tuplestore_puttuple.md)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [plperl_func_handler](plperl_func_handler.md) (src/pl/plperl/plperl.c:2477)
  - [plperl_return_next](plperl_return_next.md) (src/pl/plperl/plperl.c:3253)

## Notes and Other Information
- Function reports errors via PostgreSQL's ereport mechanism
- Uses temporary memory contexts to prevent memory accumulation during repeated calls
- Handles both composite return types (expecting hash references) and scalar types
- Supports domain types over composite types with proper validation
- The tuple store is created in the query's per-query memory context for persistence
- Memory management includes automatic cleanup of temporary allocations after each call

## Simplified Source

```c
static void
plperl_return_next_internal(SV *sv)
{
    plperl_proc_desc *prodesc;
    FunctionCallInfo fcinfo;
    ReturnSetInfo *rsi;
    MemoryContext old_cxt;

    if (!sv)
        return;

    prodesc = current_call_data->prodesc;
    fcinfo = current_call_data->fcinfo;
    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    // Verify this is a SETOF function
    if (!prodesc->fn_retisset)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("cannot use return_next in a non-SETOF function")));

    // Initialize tuple store and descriptor on first call
    if (!current_call_data->ret_tdesc) {
        TupleDesc tupdesc;

        if (prodesc->fn_retistuple) {
            // Handle composite return types
            TypeFuncClass funcclass;
            Oid typid;

            funcclass = get_call_result_type(fcinfo, &typid, &tupdesc);
            if (funcclass != TYPEFUNC_COMPOSITE && funcclass != TYPEFUNC_COMPOSITE_DOMAIN)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("function returning record called in context "
                                     "that cannot accept type record")));

            if (funcclass == TYPEFUNC_COMPOSITE_DOMAIN)
                current_call_data->cdomain_oid = typid;
        } else {
            // Handle scalar return types
            tupdesc = rsi->expectedDesc;
            if (tupdesc == NULL || tupdesc->natts != 1)
                elog(ERROR, "expected single-column result descriptor for non-composite SETOF result");
        }

        // Create persistent tuple store
        old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
        current_call_data->ret_tdesc = CreateTupleDescCopy(tupdesc);
        current_call_data->tuple_store = tuplestore_begin_heap(
            rsi->allowedModes & SFRM_Materialize_Random, false, work_mem);
        MemoryContextSwitchTo(old_cxt);
    }

    // Create temporary context for this call's allocations
    if (!current_call_data->tmp_cxt) {
        current_call_data->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                                          "PL/Perl return_next temporary cxt",
                                                          ALLOCSET_DEFAULT_SIZES);
    }

    old_cxt = MemoryContextSwitchTo(current_call_data->tmp_cxt);

    if (prodesc->fn_retistuple) {
        // Handle composite return values
        HeapTuple tuple;

        if (!(SvOK(sv) && SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVHV))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("SETOF-composite-returning PL/Perl function "
                                 "must call return_next with reference to hash")));

        tuple = plperl_build_tuple_result((HV *) SvRV(sv), current_call_data->ret_tdesc);

        // Validate domain constraints if needed
        if (OidIsValid(current_call_data->cdomain_oid))
            domain_check(HeapTupleGetDatum(tuple), false,
                        current_call_data->cdomain_oid,
                        &current_call_data->cdomain_info,
                        rsi->econtext->ecxt_per_query_memory);

        tuplestore_puttuple(current_call_data->tuple_store, tuple);
    } else if (prodesc->result_oid) {
        // Handle scalar return values
        Datum ret[1];
        bool isNull[1];

        ret[0] = plperl_sv_to_datum(sv, prodesc->result_oid, -1, fcinfo,
                                   &prodesc->result_in_func,
                                   prodesc->result_typioparam, &isNull[0]);

        tuplestore_putvalues(current_call_data->tuple_store,
                            current_call_data->ret_tdesc, ret, isNull);
    }

    // Clean up temporary allocations
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset(current_call_data->tmp_cxt);
}
```
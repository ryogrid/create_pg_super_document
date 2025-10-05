# aclexplode

## Location
[src/backend/utils/adt/acl.c:1791-1894](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1791-L1894)

## Overview
Converts an ACL (Access Control List) array into a tabular format, with one row per individual privilege grant.

## Definition
```c
Datum aclexplode(PG_FUNCTION_ARGS)
```

## Detailed Description
The `aclexplode` function is a set-returning function (SRF) that decomposes an ACL array into a detailed table format. Each row in the result represents a single privilege grant, showing the grantor, grantee, privilege type, and whether the grant includes the grant option. This function is essential for introspection of PostgreSQL's privilege system, allowing users and administrators to see exactly which privileges have been granted and by whom.

The function works by:
1. Iterating through each ACL item in the input array
2. For each ACL item, iterating through all possible privilege bits
3. For each privilege bit that is set, creating a result row
4. Including grant option information for each privilege

The output table has four columns: grantor (OID), grantee (OID), privilege_type (text), and is_grantable (boolean). This detailed breakdown makes it easy to understand complex privilege structures and relationships.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Acl*): The ACL array to explode into tabular format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ACL_P (macro for extracting ACL argument)
  - SRF_IS_FIRSTCALL/SRF_FIRSTCALL_INIT (set-returning function macros)
  - SRF_PERCALL_SETUP/SRF_RETURN_NEXT/SRF_RETURN_DONE (SRF control macros)
  - [check_acl](../c/check_acl.md) (validates ACL structure)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)/TupleDescInitEntry/BlessTupleDesc (tuple descriptor creation)
  - ACL_NUM/ACL_DAT (macros for accessing ACL components)
  - ACLITEM_GET_PRIVS/ACLITEM_GET_GOPTIONS (privilege extraction macros)
  - [convert_aclright_to_string](../c/convert_aclright_to_string.md) (converts privilege bits to strings)
  - [heap_form_tuple](../h/heap_form_tuple.md)/HeapTupleGetDatum (tuple construction)
  - Memory management functions (palloc, MemoryContextSwitchTo)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a PostgreSQL SQL function for ACL introspection
- Implements the set-returning function protocol for returning multiple rows
- Uses a state machine approach with persistent context between calls
- Each privilege bit is examined individually, creating separate rows for each granted privilege
- Handles grant options separately, showing which privileges can be further granted
- Memory management follows PostgreSQL's multi-call function context pattern
- Essential for database administration tools and privilege auditing
- The function can handle empty ACLs gracefully
- [Result](../R/Result.md) format matches PostgreSQL's standard privilege display conventions
- Used by system views and administrative functions for privilege reporting

## Simplified Source

```c
Datum aclexplode(PG_FUNCTION_ARGS) {
    Acl *acl = PG_GETARG_ACL_P(0);
    FuncCallContext *funcctx;
    int *idx;  // [0] = ACL item index, [1] = privilege bit index
    AclItem *aidat;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        check_acl(acl);
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Build result tuple descriptor: (grantor, grantee, privilege_type, is_grantable)
        tupdesc = CreateTemplateTupleDesc(4);
        TupleDescInitEntry(tupdesc, 1, "grantor", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "grantee", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "privilege_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "is_grantable", BOOLOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        // Initialize iteration state
        idx = (int *) palloc(sizeof(int[2]));
        idx[0] = 0;   // ACL array item index
        idx[1] = -1;  // privilege type counter
        funcctx->user_fctx = idx;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    idx = (int *) funcctx->user_fctx;
    aidat = ACL_DAT(acl);

    // Iterate through ACL items and privilege bits
    while (idx[0] < ACL_NUM(acl)) {
        AclItem *aidata;
        AclMode priv_bit;

        idx[1]++;
        if (idx[1] == N_ACL_RIGHTS) {
            idx[1] = 0;
            idx[0]++;
            if (idx[0] >= ACL_NUM(acl))
                break;
        }

        aidata = &aidat[idx[0]];
        priv_bit = UINT64CONST(1) << idx[1];

        // If this privilege bit is set, return a row
        if (ACLITEM_GET_PRIVS(*aidata) & priv_bit) {
            Datum values[4];
            bool nulls[4] = {0};
            HeapTuple tuple;

            values[0] = ObjectIdGetDatum(aidata->ai_grantor);
            values[1] = ObjectIdGetDatum(aidata->ai_grantee);
            values[2] = CStringGetTextDatum(convert_aclright_to_string(priv_bit));
            values[3] = BoolGetDatum((ACLITEM_GET_GOPTIONS(*aidata) & priv_bit) != 0);

            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
        }
    }

    SRF_RETURN_DONE(funcctx);
}
```
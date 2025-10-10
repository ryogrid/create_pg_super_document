# mxact

## Location
[src/backend/access/transam/multixact.c:3509-3566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3509-L3566)

## Overview
mxact is a typedef struct used within the pg_get_multixact_members function to store state information for iterating through multixact members when returning set-returning function results.

## Definition

```c
int			iter;
	} mxact;
	MultiXactId mxid = PG_GETARG_TRANSACTIONID(0);
	mxact	   *multi;
	FuncCallContext *funccxt;

	if (mxid < FirstMultiXactId)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid MultiXactId: %u", mxid)));

	if (SRF_IS_FIRSTCALL())
```
## Detailed Description
This struct serves as a context holder for the pg_get_multixact_members set-returning function (SRF). It stores the array of multixact members retrieved for a given MultiXactId, the total count of members, and an iterator index for tracking progress through the member list during successive function calls. The struct is allocated in the SRF's multi-call memory context to persist across multiple function invocations.

## Parameters / Member Variables
- : Pointer to an array of MultiXactMember structures containing the actual member data
- : Total number of members in the multixact
- : Current iteration index for tracking position when returning members one by one

## Dependencies
- Functions called/Symbols referenced:
  - Used within pg_get_multixact_members function
  - References MultiXactMember type
  - Used with SRF (Set Returning Function) infrastructure
- Called from:
  - Referenced internally within pg_get_multixact_members function

## Notes and Other Information
- This is a local typedef struct defined within pg_get_multixact_members function scope
- Used specifically for SRF state management to return multixact member information
- Memory is allocated in the function's multi-call memory context for persistence
- Part of the SQL-callable function interface for inspecting multixact contents
- Located in src/backend/access/transam/multixact.c:3509-3566

## Simplified Source

```c
Datum pg_get_multixact_members(PG_FUNCTION_ARGS) {
    // Local state struct for SRF iteration
    } mxact;
    MultiXactId mxid = PG_GETARG_TRANSACTIONID(0);
    mxact *multi;
    FuncCallContext *funccxt;

    // Validate input MultiXactId
    if (mxid < FirstMultiXactId)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("invalid MultiXactId: %u", mxid)));

    // First call: initialize SRF context and get multixact members
    if (SRF_IS_FIRSTCALL()) {
        funccxt = SRF_FIRSTCALL_INIT();
        MemoryContext oldcxt = MemoryContextSwitchTo(funccxt->multi_call_memory_ctx);

        // Allocate state and retrieve multixact members
        multi = palloc(sizeof(mxact));
        multi->nmembers = GetMultiXactIdMembers(mxid, &multi->members, false, false);
        multi->iter = 0;

        // Setup tuple descriptor for return type
        TupleDesc tupdesc;
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");
        funccxt->tuple_desc = tupdesc;
        funccxt->attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funccxt->user_fctx = multi;

        MemoryContextSwitchTo(oldcxt);
    }

    // Subsequent calls: return next member
    funccxt = SRF_PERCALL_SETUP();
    multi = (mxact *) funccxt->user_fctx;

    // Iterate through members, returning one per call
    while (multi->iter < multi->nmembers) {
        char *values[2];

        // Format member xid and status as strings
        values[0] = psprintf("%u", multi->members[multi->iter].xid);
        values[1] = mxstatus_to_string(multi->members[multi->iter].status);

        // Build and return tuple
        HeapTuple tuple = BuildTupleFromCStrings(funccxt->attinmeta, values);
        multi->iter++;
        pfree(values[0]);
        SRF_RETURN_NEXT(funccxt, HeapTupleGetDatum(tuple));
    }

    // All members returned
    SRF_RETURN_DONE(funccxt);
}
```
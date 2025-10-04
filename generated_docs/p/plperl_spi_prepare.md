# plperl_spi_prepare

## Location
[src/pl/plperl/plperl.c:3567-3714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3567-L3714)

## Overview
Prepares a parameterized SQL statement for execution, creating a reusable query plan with type information for efficient repeated execution in PL/Perl functions.

## Definition
```c
SV *plperl_spi_prepare(char *query, int argc, SV **argv)
```

## Detailed Description
This function implements prepared statement functionality for PL/Perl by creating and caching query plans with parameter type information. It provides significant performance benefits for repeatedly executed queries with parameters by avoiding repeated parsing and planning overhead.

Key operations:
1. Creates a dedicated memory context for the query descriptor and related data
2. Parses parameter type names using parseTypeString to resolve type OIDs
3. Prepares type input functions for parameter conversion using getTypeInputInfo
4. Creates the SQL plan using SPI_prepare with parameter types
5. Makes the plan persistent using SPI_keepplan for reuse across calls  
6. Stores the complete query descriptor in a hash table for fast retrieval
7. Returns a unique query identifier that can be used to execute the prepared statement

The function manages complex memory contexts to ensure proper resource lifecycle, using a permanent context for the query descriptor and plan, plus temporary workspace for preparation operations. All operations occur within a subtransaction for safe error handling.

## Parameters / Member Variables
- `query`: C string containing the SQL query with parameter placeholders (e.g., $1, $2, etc.)
- `argc`: Integer count of parameters in the query
- `argv`: Array of SV* pointers containing parameter type names as Perl scalars (e.g., "int4", "text", "timestamp")

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - AllocSetContextCreate
  - [sv2cstr](../s/sv2cstr.md)
  - [parseTypeString](parseTypeString.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [pg_verifymbstr](pg_verifymbstr.md)
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
  - [SPI_keepplan](../S/SPI_keepplan.md)
  - [hash_search](../h/hash_search.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [SPI_freeplan](../S/SPI_freeplan.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [croak_cstr](../c/croak_cstr.md)
  - [cstr2sv](../c/cstr2sv.md)
- Called from (representative examples):
  - PL_PERL_H header (src/pl/plperl/plperl.h:32)

## Notes and Other Information
- Creates persistent query plans that survive function calls for performance
- Uses hash table storage for fast query plan retrieval by unique identifier
- Comprehensive error handling with automatic cleanup of partially created resources
- Parameter types must be specified as PostgreSQL type names (e.g., "int4", "text")
- [Query](../Q/Query.md) validation ensures proper encoding before plan creation
- Memory management uses dedicated contexts to prevent leaks
- Plans are made persistent with SPI_keepplan for reuse across transactions
- Returns unique query identifier string for use with execution functions
- Supports complex parameter type resolution including domains and custom types

## Simplified Source

```c
SV *
plperl_spi_prepare(char *query, int argc, SV **argv)
{
    volatile SPIPlanPtr plan = NULL;
    volatile MemoryContext plan_cxt = NULL;
    plperl_query_desc *volatile qdesc = NULL;
    plperl_query_entry *volatile hash_entry = NULL;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldowner = CurrentResourceOwner;
    MemoryContext work_cxt;
    int i;

    check_spi_usage_allowed();

    // Execute in subtransaction for error handling
    BeginInternalSubTransaction(NULL);
    MemoryContextSwitchTo(oldcontext);

    PG_TRY();
    {
        // Create memory context for query descriptor
        plan_cxt = AllocSetContextCreate(TopMemoryContext,
                                       "PL/Perl spi_prepare query",
                                       ALLOCSET_SMALL_SIZES);
        MemoryContextSwitchTo(plan_cxt);

        // Allocate and initialize query descriptor
        qdesc = (plperl_query_desc *) palloc0(sizeof(plperl_query_desc));
        snprintf(qdesc->qname, sizeof(qdesc->qname), "%p", qdesc);
        qdesc->plan_cxt = plan_cxt;
        qdesc->nargs = argc;
        qdesc->argtypes = (Oid *) palloc(argc * sizeof(Oid));
        qdesc->arginfuncs = (FmgrInfo *) palloc(argc * sizeof(FmgrInfo));
        qdesc->argtypioparams = (Oid *) palloc(argc * sizeof(Oid));

        MemoryContextSwitchTo(oldcontext);

        // Create workspace for parameter type resolution
        work_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "PL/Perl spi_prepare workspace",
                                       ALLOCSET_DEFAULT_SIZES);
        MemoryContextSwitchTo(work_cxt);

        // Resolve parameter types
        for (i = 0; i < argc; i++)
        {
            Oid typId, typInput, typIOParam;
            int32 typmod;
            char *typstr;

            typstr = sv2cstr(argv[i]);
            (void) parseTypeString(typstr, &typId, &typmod, NULL);
            pfree(typstr);

            getTypeInputInfo(typId, &typInput, &typIOParam);

            qdesc->argtypes[i] = typId;
            fmgr_info_cxt(typInput, &(qdesc->arginfuncs[i]), plan_cxt);
            qdesc->argtypioparams[i] = typIOParam;
        }

        // Validate and prepare the query
        pg_verifymbstr(query, strlen(query), false);
        plan = SPI_prepare(query, argc, qdesc->argtypes);
        if (plan == NULL)
            elog(ERROR, "SPI_prepare() failed:%s", SPI_result_code_string(SPI_result));

        // Make plan persistent and store in hash table
        if (SPI_keepplan(plan))
            elog(ERROR, "SPI_keepplan() failed");
        qdesc->plan = plan;

        hash_entry = hash_search(plperl_active_interp->query_hash,
                               qdesc->qname, HASH_ENTER, NULL);
        hash_entry->query_data = qdesc;

        // Clean up workspace
        MemoryContextDelete(work_cxt);

        // Commit subtransaction
        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcontext);
        CurrentResourceOwner = oldowner;
    }
    PG_CATCH();
    {
        // Handle errors: cleanup and propagate to Perl
        ErrorData *edata;
        MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();

        // Clean up allocated resources
        if (hash_entry)
            hash_search(plperl_active_interp->query_hash, qdesc->qname, HASH_REMOVE, NULL);
        if (plan_cxt)
            MemoryContextDelete(plan_cxt);
        if (plan)
            SPI_freeplan(plan);

        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcontext);
        CurrentResourceOwner = oldowner;

        croak_cstr(edata->message);
        return NULL;
    }
    PG_END_TRY();

    return cstr2sv(qdesc->qname);
}
```
# pg_get_keywords

## Location
[src/backend/utils/adt/misc.c:418-495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L418-L495)

## Overview
Returns information about all SQL grammar keywords recognized by PostgreSQL, including their reservation status and label usage rules.

## Definition
```c
Datum pg_get_keywords(PG_FUNCTION_ARGS)
```

## Detailed Description
This set-returning function provides comprehensive information about PostgreSQL's SQL keywords. It iterates through the ScanKeywords table and returns details about each keyword including:

1. **Keyword name**: The actual keyword string
2. **Category code**: Single-character code indicating reservation level (U/C/T/R)  
3. **Bare label flag**: Boolean indicating if keyword can be used as a bare label
4. **Category description**: Human-readable description of the reservation level
5. **Label description**: Human-readable description of label usage rules

The function categorizes keywords into four types:
- **UNRESERVED_KEYWORD** (U): Can be used freely as identifiers
- **COL_NAME_KEYWORD** (C): Can be column names but not function/type names
- **TYPE_FUNC_NAME_KEYWORD** (T): Reserved but can still be function/type names  
- **RESERVED_KEYWORD** (R): Fully reserved, cannot be used as identifiers

The function also indicates whether each keyword can be used as a "bare label" (without AS keyword) in certain SQL contexts.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT  
  - SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
  - [get_call_result_type](../g/get_call_result_type.md)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [GetScanKeyword](../G/GetScanKeyword.md)
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - unconstify
- Global data structures referenced:
  - ScanKeywords
  - ScanKeywordCategories  
  - ScanKeywordBareLabel
- Constants referenced:
  - UNRESERVED_KEYWORD
  - COL_NAME_KEYWORD
  - TYPE_FUNC_NAME_KEYWORD
  - RESERVED_KEYWORD
  - TYPEFUNC_COMPOSITE
- Called from:
  - SQL function calls (no direct C references found)

## Notes and Other Information
- This function powers the pg_get_keywords() SQL function used by tools like psql's \dS command
- The returned data helps applications determine safe identifier usage and keyword conflicts
- Uses the standard PostgreSQL SRF (Set Returning Function) framework for iterating through results
- The keyword data comes from the scanner's built-in keyword tables which are generated during build
- Internationalization support through gettext (_() macro) for description strings
- Memory management handled through SRF framework's multi-call context
- The 'bare label' concept relates to SQL syntax where some keywords can appear without AS in certain contexts

## Simplified Source

```c
Datum pg_get_keywords(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;

    if (SRF_IS_FIRSTCALL()) {
        // Initialize set-returning function context
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Validate return type and setup tuple descriptor
        TupleDesc tupdesc;
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");

        funcctx->tuple_desc = tupdesc;
        funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    // Return next keyword if available
    if (funcctx->call_cntr < ScanKeywords.num_keywords) {
        char *values[5];

        // Get keyword name
        values[0] = unconstify(char *,
                GetScanKeyword(funcctx->call_cntr, &ScanKeywords));

        // Set category code and description based on keyword type
        switch (ScanKeywordCategories[funcctx->call_cntr]) {
            case UNRESERVED_KEYWORD:
                values[1] = "U";
                values[3] = _("unreserved");
                break;
            case COL_NAME_KEYWORD:
                values[1] = "C";
                values[3] = _("unreserved (cannot be function or type name)");
                break;
            case TYPE_FUNC_NAME_KEYWORD:
                values[1] = "T";
                values[3] = _("reserved (can be function or type name)");
                break;
            case RESERVED_KEYWORD:
                values[1] = "R";
                values[3] = _("reserved");
                break;
            default:
                values[1] = NULL;
                values[3] = NULL;
                break;
        }

        // Set bare label information
        if (ScanKeywordBareLabel[funcctx->call_cntr]) {
            values[2] = "true";
            values[4] = _("can be bare label");
        } else {
            values[2] = "false";
            values[4] = _("requires AS");
        }

        HeapTuple tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
```
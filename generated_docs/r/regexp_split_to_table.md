# regexp_split_to_table

## Location
[src/backend/utils/adt/regexp.c:1702-1754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1702-L1754)

## Overview
Splits a string at matches of a regular expression pattern, returning the split-out substrings as a table (set-returning function).

## Definition

```c
Datum
regexp_split_to_table(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements a PostgreSQL set-returning function (SRF) that splits an input string using a regular expression pattern and returns each split substring as a separate row. The function uses the SRF framework to manage state across multiple calls, storing the regexp matching context in the function's multi-call memory context. Unlike the standard behavior where users can specify the 'g' (global) flag, this function prohibits the global flag but internally forces global matching to find all occurrences of the pattern. The function processes matches sequentially, returning one split result per call until all matches have been processed.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: Input text string to split
  - Argument 1: Regular expression pattern (text)
  - Argument 2: Optional regex flags (text), but 'g' flag is prohibited

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT, SRF_RETURN_DONE
  - PG_GETARG_TEXT_PP, PG_GETARG_TEXT_PP_IF_EXISTS, PG_GETARG_TEXT_P_COPY
  - PG_GET_COLLATION
  - [parse_re_flags](../p/parse_re_flags.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [build_regexp_split_result](../b/build_regexp_split_result.md)
  - [FuncCallContext](../F/FuncCallContext.md), regexp_matches_ctx, pg_re_flags
- Called from:
  - [regexp_split_to_table_no_flags](regexp_split_to_table_no_flags.md)

## Notes and Other Information
- Prohibits the 'g' (global) flag in user input but internally enables global matching
- Uses PostgreSQL's SRF (Set Returning Function) framework for returning multiple rows
- Maintains state across calls using regexp_matches_ctx stored in multi_call_memory_ctx
- Each call returns one split substring, with the function completing when all matches are processed
- Located at src/backend/utils/adt/regexp.c:1702-1754

## Simplified Source

```c
Datum regexp_split_to_table(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    regexp_matches_ctx *splitctx;

    if (SRF_IS_FIRSTCALL()) {
        text *pattern = PG_GETARG_TEXT_PP(1);
        text *flags = PG_GETARG_TEXT_PP_IF_EXISTS(2);

        funcctx = SRF_FIRSTCALL_INIT();

        // Parse regex flags but reject global flag
        pg_re_flags re_flags;
        parse_re_flags(&re_flags, flags);

        if (re_flags.glob)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("regexp_split_to_table() does not support the \"global\" option")));

        // Force global matching internally to find all splits
        re_flags.glob = true;

        // Setup regexp matching for splitting (fetching_unmatched = true)
        splitctx = setup_regexp_matches(PG_GETARG_TEXT_P_COPY(0), pattern,
                                       &re_flags, 0, PG_GET_COLLATION(),
                                       false, true, true);

        funcctx->user_fctx = splitctx;
    }

    funcctx = SRF_PERCALL_SETUP();
    splitctx = (regexp_matches_ctx *) funcctx->user_fctx;

    // Return next split substring if available
    if (splitctx->next_match <= splitctx->nmatches) {
        Datum result = build_regexp_split_result(splitctx);
        splitctx->next_match++;
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}
```
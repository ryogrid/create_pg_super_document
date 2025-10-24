# test_regex

## Location
[src/test/modules/test_regex/test_regex.c:80-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_regex/test_regex.c#L80-L160)

## Overview
test_regex is a PostgreSQL set-returning function (SRF) that provides comprehensive regex testing functionality, offering detailed information about pattern matching similar to Tcl's "regexp -about" output.

## Definition
Datum test_regex(PG_FUNCTION_ARGS)

## Detailed Description
This function implements a PostgreSQL set-returning function that takes a regex pattern, input text, and flags as arguments. It returns multiple rows of results: the first row contains information about the compiled regex pattern (equivalent to Tcl's "regexp -about" output), and subsequent rows describe each match found in the input text.

The function operates in two phases:
1. **First call (SRF_IS_FIRSTCALL)**: Compiles the regex pattern, sets up matching context, and returns pattern information
2. **Subsequent calls**: Returns details about each individual match found in the input text

The function uses PostgreSQL's SRF infrastructure to manage state between calls and efficiently return multiple result rows.

## Parameters / Member Variables
-  (pattern): Text containing the regular expression pattern to compile and use
-  (input text): Text to search for matches against the pattern  
-  (flags): Text containing regex compilation and execution flags

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL (PostgreSQL SRF macro)
  - SRF_FIRSTCALL_INIT (PostgreSQL SRF initialization)
  - SRF_PERCALL_SETUP (PostgreSQL SRF per-call setup)
  - [parse_test_flags](../p/parse_test_flags.md) (parses flag arguments)
  - [test_re_compile](test_re_compile.md) (compiles regex pattern)
  - [setup_test_matches](../s/setup_test_matches.md) (sets up match execution context)
  - build_test_info_result (builds pattern info result)
  - [build_test_match_result](../b/build_test_match_result.md) (builds individual match result)
  - [pg_regfree](../p/pg_regfree.md) (frees compiled regex)
  - PG_GET_COLLATION (gets collation for pattern compilation)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's test_regex module for testing regular expression functionality
- Uses PostgreSQL's memory context management for proper cleanup
- Implements the SRF protocol correctly with proper state management
- The first result row provides metadata about the compiled pattern
- Subsequent rows provide detailed information about each match found
- Located in src/test/modules/test_regex/test_regex.c:80-160

## Simplified Source

```c
Datum test_regex(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    test_regex_ctx *matchctx;
    ArrayType *result_ary;

    if (SRF_IS_FIRSTCALL()) {
        // First call: setup and compile pattern
        text *pattern = PG_GETARG_TEXT_PP(0);
        text *flags = PG_GETARG_TEXT_PP(2);
        Oid collation = PG_GET_COLLATION();
        test_re_flags re_flags;
        regex_t cpattern;

        // Initialize SRF context
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Parse flags and compile pattern
        parse_test_flags(&re_flags, flags);
        test_re_compile(pattern, re_flags.cflags, collation, &cpattern);

        // Setup match context and workspace
        matchctx = setup_test_matches(PG_GETARG_TEXT_P_COPY(1), &cpattern,
                                      &re_flags, collation, true);
        matchctx->elems = (Datum *) palloc(sizeof(Datum) * (matchctx->npatterns + 1));
        matchctx->nulls = (bool *) palloc(sizeof(bool) * (matchctx->npatterns + 1));

        MemoryContextSwitchTo(oldcontext);
        funcctx->user_fctx = (void *) matchctx;

        // Return pattern info (equivalent to "regexp -about")
        result_ary = build_test_info_result(&cpattern, &re_flags);
        pg_regfree(&cpattern);
        SRF_RETURN_NEXT(funcctx, PointerGetDatum(result_ary));
    } else {
        // Subsequent calls: return match details
        funcctx = SRF_PERCALL_SETUP();
        matchctx = (test_regex_ctx *) funcctx->user_fctx;

        if (matchctx->next_match < matchctx->nmatches) {
            result_ary = build_test_match_result(matchctx);
            matchctx->next_match++;
            SRF_RETURN_NEXT(funcctx, PointerGetDatum(result_ary));
        }
    }

    SRF_RETURN_DONE(funcctx);
}
```
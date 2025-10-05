# tt_process_call

## Location
[src/backend/tsearch/wparser.c:77-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L77-L105)

## Overview
Processes individual calls in a multi-call function context to return token type information as tuples for PostgreSQL text search parsers.

## Definition

```c
static Datum
tt_process_call(FuncCallContext *funcctx)
```
## Detailed Description
This function handles subsequent calls (after the first call) in PostgreSQL multi-call functions that return token type information from text search parsers. It retrieves the next token type from the stored list in the function context and formats it as a tuple containing the token ID, alias, and description. The function manages iteration through the token type list and handles memory cleanup for dynamically allocated strings.

The function operates by:
1. Retrieving the TSTokenTypeStorage from the function context
2. Checking if there are more token types to process
3. Formatting the current token type data into a string array
4. Building a tuple from the formatted data
5. Cleaning up allocated memory and advancing to the next token
6. Returning the tuple as a Datum, or 0 when no more tokens exist

## Parameters / Member Variables
- `*funcctx`: Function call context containing the TSTokenTypeStorage and tuple metadata
## Dependencies
- Functions called/Symbols referenced:
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - sprintf
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ts_token_type_byid](ts_token_type_byid.md)
  - [ts_token_type_byname](ts_token_type_byname.md)

## Notes and Other Information
- This is a static function internal to the wparser.c module
- Works in conjunction with tt_setup_firstcall to implement the multi-call function protocol
- Returns (Datum) 0 to signal the end of the result set when no more tokens are available
- Properly manages memory by freeing the alias and description strings after tuple creation
- The token ID is converted to a string representation for tuple construction
- The function assumes the token type list was properly initialized by tt_setup_firstcall

## Simplified Source

```c
static Datum
tt_process_call(FuncCallContext *funcctx)
{
    TSTokenTypeStorage *st = (TSTokenTypeStorage *) funcctx->user_fctx;

    // Check if more token types available
    if (st->list && st->list[st->cur].lexid)
    {
        Datum result;
        char *values[3];
        char txtid[16];
        HeapTuple tuple;

        // Format token type data as strings
        sprintf(txtid, "%d", st->list[st->cur].lexid);
        values[0] = txtid;
        values[1] = st->list[st->cur].alias;
        values[2] = st->list[st->cur].descr;

        // Build tuple and convert to Datum
        tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
        result = HeapTupleGetDatum(tuple);

        // Clean up allocated strings and advance cursor
        pfree(values[1]);
        pfree(values[2]);
        st->cur++;

        return result;
    }

    // End of token type list
    return (Datum) 0;
}
```
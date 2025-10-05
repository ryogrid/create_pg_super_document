# ts_token_type_byid

## Location
[src/backend/tsearch/wparser.c:106-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L106-L124)

## Overview
A PostgreSQL SQL-callable function that returns token type information for a specified text search parser as a set of rows.

## Definition

```c
typedef struct
{
	int			type;
	char	   *lexeme;
} LexemeEntry;
```
## Detailed Description
This function provides a SQL interface for retrieving token type information from PostgreSQL text search parsers. It implements the Set Returning Function (SRF) protocol to return multiple rows of token type data, where each row contains the token ID, alias, and description. The function takes a parser OID as input and uses the parser's lextype method to obtain the complete list of supported token types.

The function follows PostgreSQL's standard SRF pattern:
1. On the first call, it initializes the function context and retrieves token type data
2. On subsequent calls, it returns the next token type in the sequence
3. When all token types have been returned, it signals completion

This function is typically called from SQL queries to inspect the token types supported by a particular text search parser, which is useful for understanding how the parser categorizes different types of text elements.

## Parameters
- `parser_oid`: OID of the text search parser whose token types should be returned


## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - [tt_setup_firstcall](tt_setup_firstcall.md)
  - SRF_PERCALL_SETUP
  - [tt_process_call](tt_process_call.md)
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
- Called from (representative examples):
  - Direct SQL calls from user queries
  - System catalog queries

## Notes and Other Information
- This is a public function accessible from SQL as part of PostgreSQL's text search functionality
- Uses PostgreSQL's Set Returning Function macros for proper multi-call protocol implementation
- The function name follows PostgreSQL's naming convention for text search functions (ts_*)
- Returns a composite type with columns for token ID, alias, and description
- Requires a valid text search parser OID as input parameter
- Part of PostgreSQL's full-text search infrastructure for parser introspection

## Simplified Source

```c
Datum ts_token_type_byid(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    Datum result;

    // First call: initialize context and setup token type data
    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        tt_setup_firstcall(funcctx, fcinfo, PG_GETARG_OID(0));
    }

    funcctx = SRF_PERCALL_SETUP();

    // Process each call: return next token type or signal completion
    if ((result = tt_process_call(funcctx)) != (Datum) 0)
        SRF_RETURN_NEXT(funcctx, result);

    SRF_RETURN_DONE(funcctx);
}
```
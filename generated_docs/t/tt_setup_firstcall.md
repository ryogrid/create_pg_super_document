# tt_setup_firstcall

## Location
[src/backend/tsearch/wparser.c:47-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L47-L76)

## Overview
Initializes function context and retrieves token type information for PostgreSQL text search parser functions during their first call.

## Definition


## Detailed Description
This function sets up the necessary data structures and context for PostgreSQL text search parser functions that return token type information. It is called during the first invocation of functions like  and  to initialize the function context with token type data from the specified parser. The function operates in the multi-call context memory to persist data across multiple function calls.

The function performs several key operations:
1. Validates that the parser has a lextype method defined
2. Switches to the multi-call memory context for persistent storage
3. Allocates and initializes a TSTokenTypeStorage structure
4. Retrieves the list of token types by calling the parser's lextype function
5. Sets up tuple descriptor and attribute metadata for the return type

## Parameters / Member Variables
- : Function call context structure used for multi-call functions
- : Function call information containing metadata about the function call
- : OID of the text search parser to retrieve token types from

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md)
  - OidFunctionCall1
  - [get_call_result_type](../g/get_call_result_type.md)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [ts_token_type_byid](ts_token_type_byid.md)
  - [ts_token_type_byname](ts_token_type_byname.md)

## Notes and Other Information
- This is a static function internal to the wparser.c module
- The function expects the parser to have a valid lextype method, throwing an error if not found
- Uses PostgreSQL's multi-call function framework for returning sets of rows
- Memory allocation is done in the multi-call memory context to ensure persistence across calls
- The lextype function is called with a dummy argument (Datum 0) as required by the interface
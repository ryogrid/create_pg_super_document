# ts_match_qv

## Location
[src/backend/utils/adt/tsvector_op.c:2206-2213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2206-L2213)

## Overview
A PostgreSQL function that performs text search matching between a tsquery and tsvector by delegating to ts_match_vq with swapped argument order.

## Definition

```c
Datum
ts_match_qv(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a wrapper for the @@ operator when used with argument order tsquery @@ tsvector. It simply swaps the arguments and delegates the actual matching logic to ts_match_vq, which expects the arguments in tsvector @@ tsquery order. This design pattern allows PostgreSQL to support both syntactic forms of the text search match operator while maintaining a single implementation.

The function is part of PostgreSQL's text search boolean operations infrastructure and follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS macro.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro which provides access to:
  - Argument 0: tsquery (the search query)
  - Argument 1: tsvector (the document vector to search)

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2
  - [ts_match_vq](ts_match_vq.md)
  - PG_RETURN_DATUM
  - PG_GETARG_DATUM
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- Implements the tsquery @@ tsvector variant of the text search match operator
- Acts as a simple argument-swapping wrapper around ts_match_vq
- Part of PostgreSQL's operator overloading system for text search
- Returns a Datum (PostgreSQL's generic data type) containing the boolean match result

## Simplified Source

```c
Datum ts_match_qv(PG_FUNCTION_ARGS) {
    // Swap arguments and delegate to ts_match_vq
    // tsquery @@ tsvector -> tsvector @@ tsquery
    PG_RETURN_DATUM(DirectFunctionCall2(ts_match_vq,
                                        PG_GETARG_DATUM(1),  // tsvector
                                        PG_GETARG_DATUM(0))); // tsquery
}
```
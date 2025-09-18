# JumbleQuery

## Location
src/backend/nodes/queryjumblefuncs.c: 105 - 149

## Overview
JumbleQuery is the main function that generates a unique query identifier (queryId) by creating a normalized representation of a SQL query structure and computing its hash.

## Definition


## Detailed Description
JumbleQuery processes a parsed SQL query (Query node) to generate a unique identifier that can be used to group similar queries together regardless of parameter values or formatting differences. It creates a JumbleState workspace, recursively processes the query tree structure using _jumbleNode to create a normalized representation, and then computes a hash value that becomes the query's unique identifier. The function handles edge cases where the hash might be zero by using fallback values (1 for normal statements, 2 for utility statements).

## Parameters / Member Variables
- : Input Query node representing the parsed SQL statement for which to generate an identifier

## Dependencies
- Functions called/Symbols referenced:
  - IsQueryIdEnabled (checks if query ID generation is enabled)
  - _jumbleNode (recursively processes query tree nodes)
  - hash_any_extended (computes hash from jumbled data)
  - DatumGetUInt64 (converts hash result to uint64)
  - JumbleState, JUMBLE_SIZE, LocationLen (data structures and constants)
- Called from (representative examples):
  - ExplainQuery (for EXPLAIN statement processing)
  - parse_analyze_fixedparams, parse_analyze_varparams, parse_analyze_withcb (during query analysis)
  - COMPUTE_QUERY_ID_REGRESS (testing macro)

## Notes and Other Information
- Returns a JumbleState containing the jumbled representation and location information
- Sets the queryId field directly on the input Query node
- Allocates workspace memory for jumbling operations (JUMBLE_SIZE bytes)
- Maintains constant location tracking for parameter placeholders
- Uses zero-collision avoidance: assigns 1 for regular queries, 2 for utility statements when hash is zero
- Part of PostgreSQL's query normalization system used by pg_stat_statements and query planning
- Requires query ID generation to be enabled (checked via IsQueryIdEnabled)
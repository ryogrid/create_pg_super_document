# nodeToStringWithLocations

## Location
[src/backend/nodes/outfuncs.c:797-807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L797-L807)

## Overview
A public interface function that converts a PostgreSQL node structure to its ASCII string representation with location field information included for debugging purposes.

## Definition
char *nodeToStringWithLocations(const void *obj)

## Detailed Description
nodeToStringWithLocations is an externally visible entry point for converting PostgreSQL parse tree nodes and other structures into their string representations while preserving location information. It serves as a wrapper around nodeToStringInternal, specifically calling it with the write_loc_fields parameter set to true. This means that location fields in the output will show their actual values rather than -1, which can be extremely valuable for debugging parsing and execution issues. The location information typically refers to character positions in the original SQL query text.

## Parameters / Member Variables
- obj: A pointer to the PostgreSQL node or structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - [nodeToStringInternal](nodeToStringInternal.md)
- Called from (representative examples):
  - [print](../p/print.md) (src/backend/nodes/print.c:41)
  - [pprint](../p/pprint.md) (src/backend/nodes/print.c:59)
  - [elog_node_display](../e/elog_node_display.md) (src/backend/nodes/print.c:77)
  - [pg_parse_query](../p/pg_parse_query.md) (src/backend/tcop/postgres.c:648)
  - [pg_rewrite_query](../p/pg_rewrite_query.md) (src/backend/tcop/postgres.c:856)
  - [pg_plan_query](../p/pg_plan_query.md) (src/backend/tcop/postgres.c:938)

## Notes and Other Information
- This function is primarily used for debugging and development purposes
- Location fields show actual character positions in the original query, making it useful for error reporting and query analysis
- The function is commonly used in PostgreSQL's debugging and logging infrastructure
- It is called by the print and pprint functions which are used for displaying parse trees during debugging
- Also used in main query processing functions (pg_parse_query, pg_rewrite_query, pg_plan_query) likely for debug logging
- Memory for the returned string is allocated using PostgreSQL's palloc mechanism
- Should only be used when location information is specifically needed, as it can make the output more verbose
# to_jsonb_is_immutable

## Location
[src/backend/utils/adt/jsonb.c:1049-1087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1049-L1087)

## Overview
A function that determines whether a given PostgreSQL type is immutable when converted to JSONB format, which is important for query optimization and caching decisions.

## Definition

```c
bool
to_jsonb_is_immutable(Oid typoid)
```
## Detailed Description
The to_jsonb_is_immutable function evaluates whether a PostgreSQL data type produces deterministic (immutable) output when converted to JSONB format. This determination is crucial for the query optimizer to decide whether expressions involving JSONB conversion can be cached, precomputed, or moved around in the query plan. The function categorizes the input type using json_categorize_type and then applies specific immutability rules based on the JSON type category. Date/time types are considered mutable because they depend on timezone settings, while basic types like booleans and nulls are immutable.

## Parameters / Member Variables
- : The PostgreSQL OID of the data type to be evaluated for immutability

## Dependencies
- Functions called/Symbols referenced:
  - [json_categorize_type](../j/json_categorize_type.md)
  - [func_volatile](../f/func_volatile.md)
  - JsonTypeCategory
  - JSONTYPE_NULL, JSONTYPE_BOOL, JSONTYPE_JSON, JSONTYPE_JSONB
  - JSONTYPE_DATE, JSONTYPE_TIMESTAMP, JSONTYPE_TIMESTAMPTZ
  - JSONTYPE_ARRAY, JSONTYPE_COMPOSITE
  - JSONTYPE_NUMERIC, JSONTYPE_CAST, JSONTYPE_OTHER
  - PROVOLATILE_IMMUTABLE
- Called from (representative examples):
  - [contain_mutable_functions_walker](../c/contain_mutable_functions_walker.md)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Returns true for basic immutable types: NULL, BOOL, JSON, JSONB
- Returns false for date/time types due to timezone dependency
- Currently returns false for arrays and composite types with TODO comments indicating future plans to recurse into their elements/fields
- For numeric, cast, and other types, delegates to func_volatile() to check the output function's volatility
- This function is essential for PostgreSQL's query optimization, particularly for determining when JSONB expressions can be treated as constants
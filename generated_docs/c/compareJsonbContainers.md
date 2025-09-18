# compareJsonbContainers

## Location
[src/backend/utils/adt/jsonb_util.c:191-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L191-L340)

## Overview
B-Tree comparator function that performs lexicographic comparison of two JSONB containers, returning an integer indicating their relative order for sorting and indexing operations.

## Definition
```c
int compareJsonbContainers(JsonbContainer *a, JsonbContainer *b)
```

## Detailed Description
This is the core comparison function for JSONB values, designed to be consistent with B-Tree operator class requirements. It performs a deep, element-by-element comparison of two JSONB containers using iterators to traverse their structure.

The comparison algorithm:
1. **Parallel iteration**: Uses JsonbIterator to traverse both containers simultaneously
2. **Token-based comparison**: Compares iterator tokens (BEGIN_ARRAY, END_OBJECT, etc.) and their associated values
3. **Type-based ordering**: When types differ, applies a consistent type hierarchy for ordering
4. **Scalar comparison**: Uses compareJsonbScalarValue for primitive values (strings, numbers, booleans, null)
5. **Container size comparison**: For arrays and objects, compares element/pair counts when types match
6. **Memory management**: Properly cleans up iterator chains to prevent memory leaks

The function handles special cases like "raw scalar" pseudo-arrays and ensures consistent ordering for heterogeneous JSONB structures.

## Parameters / Member Variables
- `a`: First JsonbContainer to compare
- `b`: Second JsonbContainer to compare

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md) (initializes JSONB iteration)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md) (advances iterator and gets next value)
  - [compareJsonbScalarValue](compareJsonbScalarValue.md) (compares scalar JSONB values)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - elog (error logging)
- Called from (representative examples):
  - [jsonb_eq](../j/jsonb_eq.md) (equality operator)
  - [jsonb_lt](../j/jsonb_lt.md) (less-than operator)
  - [jsonb_gt](../j/jsonb_gt.md) (greater-than operator)
  - [jsonb_le](../j/jsonb_le.md) (less-than-or-equal operator)
  - [jsonb_ge](../j/jsonb_ge.md) (greater-than-or-equal operator)
  - [jsonb_cmp](../j/jsonb_cmp.md) (comparison function)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Returns negative integer if a < b, zero if a == b, positive integer if a > b
- Implements lexicographic ordering suitable for B-Tree indexing operations
- Handles complex nested structures through recursive iterator traversal
- Memory-safe with proper cleanup of iterator chains
- Errors on unexpected jbvBinary and jbvDatetime value types during comparison
- Essential for JSONB ordering operations, equality tests, and index maintenance
- Located in src/backend/utils/adt/jsonb_util.c:191-340
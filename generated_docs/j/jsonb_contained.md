# jsonb_contained

## Location
[src/backend/utils/adt/jsonb_op.c:130-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_op.c#L130-L148)

## Overview
Tests whether a JSONB value is contained within another JSONB value as a subset, implementing the commutator of the contains operation.

## Definition

```c
Datum
jsonb_contained(PG_FUNCTION_ARGS)
```
## Detailed Description
The jsonb_contained function implements the PostgreSQL '<@' operator for JSONB values. It is the commutator of the jsonb_contains function, testing whether the first JSONB value is contained within the second JSONB value as a subset. This performs the same deep containment checking as jsonb_contains but with the arguments reversed.

The function swaps the argument order compared to jsonb_contains: it checks if the first argument (template) is contained within the second argument (value). This makes '<@' the logical inverse of '@>' in terms of argument order.

## Parameters / Member Variables
-  (Jsonb *): The template JSONB value that should be contained within the second argument
-  (Jsonb *): The JSONB value that potentially contains the template

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_OBJECT
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbDeepContains](../J/JsonbDeepContains.md)
  - PG_RETURN_BOOL
- Types used:
  - Jsonb
  - JsonbIterator

## Notes and Other Information
- Implements the commutator relationship: A <@ B is equivalent to B @> A
- Returns false immediately if root types don't match (object vs array)
- Uses the same JsonbDeepContains logic as jsonb_contains but with swapped arguments
- Corresponds to the '<@' operator in PostgreSQL JSONB operations
- The comment in the code explicitly notes this is the "Commutator of contains"
- Provides syntactic convenience for expressing containment relationships in different ways

## Simplified Source

```c
Datum jsonb_contained(PG_FUNCTION_ARGS) {
    // Commutator of "contains" - swapped argument order
    Jsonb *tmpl = PG_GETARG_JSONB_P(0);  // Template to be contained
    Jsonb *val = PG_GETARG_JSONB_P(1);   // Container value

    JsonbIterator *it1, *it2;

    // Quick check: root types must match (object vs array)
    if (JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl))
        PG_RETURN_BOOL(false);

    // Initialize iterators: val contains tmpl
    it1 = JsonbIteratorInit(&val->root);
    it2 = JsonbIteratorInit(&tmpl->root);

    // Check if val contains tmpl (same logic as jsonb_contains)
    PG_RETURN_BOOL(JsonbDeepContains(&it1, &it2));
}
```
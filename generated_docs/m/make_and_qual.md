# make_and_qual

## Location
[src/backend/nodes/makefuncs.c:754-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L754-L772)

## Overview
A specialized variant of `make_andclause` designed for combining two qualifier conditions with NULL-handling semantics where NULL represents 'true'.

## Definition
```c
Node *make_and_qual(Node *qual1, Node *qual2)
```

## Detailed Description
The `make_and_qual` function provides a convenient way to AND together two qualifier conditions with special handling for NULL values. In PostgreSQL's query processing, NULL qualifiers are interpreted as 'true', meaning they don't impose any restriction. This function implements that logic by returning the non-NULL qualifier when one is NULL, or creating an AND clause when both are non-NULL.

This function serves as a higher-level convenience wrapper around `make_andclause` for the common case of combining exactly two conditions. It handles the NULL cases efficiently without creating unnecessary expression nodes, and only creates an AND clause when both qualifiers are present.

The function includes an important caveat: it makes no attempt to preserve AND/OR flatness, so it should not be used on qualifiers that have already been processed through prepqual.c, which performs various normalizations including flattening of nested boolean expressions.

## Parameters / Member Variables
- `qual1`: First Node pointer representing a qualifier condition (NULL interpreted as 'true')
- `qual2`: Second Node pointer representing a qualifier condition (NULL interpreted as 'true')

## Dependencies
- Functions called/Symbols referenced:
  - [make_andclause](make_andclause.md) (to create the AND expression when both qualifiers are present)
  - list_make2 (to create a two-element list for the AND clause arguments)
- Called from (representative examples):
  - [subquery_push_qual](../s/subquery_push_qual.md) (in query optimization)
  - [transform_MERGE_to_join](../t/transform_MERGE_to_join.md) (in join processing)
  - [AddQual](../A/AddQual.md) (in query rewriting)

## Notes and Other Information
- Returns the non-NULL qualifier directly when the other is NULL, avoiding unnecessary expression creation
- Only creates an AND clause when both qualifiers are non-NULL
- Should not be used on qualifiers that have undergone prepqual.c processing due to flattening concerns
- Commonly used in query rewriting and optimization phases where conditions need to be combined incrementally
- The NULL-as-true semantics are specific to PostgreSQL's internal qualifier representation
- More efficient than always creating AND clauses since it avoids node creation in common cases

## Simplified Source

```c
Node *
make_and_qual(Node *qual1, Node *qual2)
{
    // NULL qualifiers represent 'true' - return the other qualifier
    if (qual1 == NULL)
        return qual2;
    if (qual2 == NULL)
        return qual1;

    // Both qualifiers present - create AND clause
    return (Node *) make_andclause(list_make2(qual1, qual2));
}
```
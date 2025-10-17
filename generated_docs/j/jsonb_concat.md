# jsonb_concat

## Location
[src/backend/utils/adt/jsonfuncs.c:4599-4639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4599-L4639)

## Overview
Concatenates two JSONB values, implementing the PostgreSQL || operator for JSONB types by merging objects or arrays.

## Definition
```c
Datum jsonb_concat(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_concat` function implements the || concatenation operator for JSONB values. It handles different combination scenarios:

- **Object + Object**: Merges two objects, with the second objects key-value pairs taking precedence over the first in case of conflicts
- **Array + Array**: Concatenates arrays into a single array
- **Mixed types**: Returns appropriate combined result based on the input types

The function optimizes for empty inputs by returning the non-empty operand when possible, avoiding unnecessary processing.

## Parameters / Member Variables
- `jb1`: First JSONB value to concatenate
- `jb2`: Second JSONB value to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_OBJECT
  - JB_ROOT_COUNT
  - JB_ROOT_IS_SCALAR
  - JsonbIteratorInit
  - IteratorConcat
  - JsonbValueToJsonb
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Optimizes for empty operands to avoid unnecessary processing
- Uses IteratorConcat for the core concatenation logic
- Handles type compatibility checks between operands
- Exposed as the SQL function underlying the || operator for JSONB types

## Simplified Source

```c
Datum jsonb_concat(PG_FUNCTION_ARGS) {
    Jsonb *jb1 = PG_GETARG_JSONB_P(0);
    Jsonb *jb2 = PG_GETARG_JSONB_P(1);
    JsonbParseState *state = NULL;
    JsonbValue *res;
    JsonbIterator *it1, *it2;

    // Optimization: return non-empty operand if the other is empty and compatible
    if (JB_ROOT_IS_OBJECT(jb1) == JB_ROOT_IS_OBJECT(jb2)) {
        if (JB_ROOT_COUNT(jb1) == 0 && !JB_ROOT_IS_SCALAR(jb2))
            PG_RETURN_JSONB_P(jb2);
        else if (JB_ROOT_COUNT(jb2) == 0 && !JB_ROOT_IS_SCALAR(jb1))
            PG_RETURN_JSONB_P(jb1);
    }

    // Initialize iterators for both operands
    it1 = JsonbIteratorInit(&jb1->root);
    it2 = JsonbIteratorInit(&jb2->root);

    // Perform concatenation using core logic
    res = IteratorConcat(&it1, &it2, &state);

    PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}
```
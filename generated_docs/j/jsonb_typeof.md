# jsonb_typeof

## Location
[src/backend/utils/adt/jsonb.c:222-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L222-L235)

## Overview
A SQL function that returns the type name of a JSONB value as text.

## Definition

```c
Datum
jsonb_typeof(PG_FUNCTION_ARGS)
```

## Simplified Source

```c
Datum
jsonb_typeof(PG_FUNCTION_ARGS)
{
    // Get the input JSONB value
    Jsonb *in = PG_GETARG_JSONB_P(0);

    // Get the type name from the JSONB container
    const char *result = JsonbContainerTypeName(&in->root);

    // Convert to text and return
    PG_RETURN_TEXT_P(cstring_to_text(result));
}
```
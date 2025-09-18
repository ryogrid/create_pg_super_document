# jsonb_concat

## Location
src/backend/utils/adt/jsonfuncs.c: 4599 - 4639

## Overview
Concatenates two JSONB values, implementing the PostgreSQL || operator for JSONB types by merging objects or arrays.

## Definition
```c
Datum jsonb_concat(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_concat` function implements the || concatenation operator for JSONB values. It handles different combination scenarios:

- **Object + Object**: Merges two objects, with the second objects key-value pairs taking precedence over the first in case of conflicts
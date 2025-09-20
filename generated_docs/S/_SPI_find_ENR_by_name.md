# _SPI_find_ENR_by_name

## Location
[src/backend/executor/spi.c:3280-3296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3280-L3296)

## Overview
Internal function that looks up an ephemeral named relation by name within the current SPI execution context.

## Definition

```c
static EphemeralNamedRelation
_SPI_find_ENR_by_name(const char *name)
```
## Detailed Description
This is an internal static function used by the SPI (Server Programming Interface) system to locate ephemeral named relations (ENRs) by their string name. The function provides a simple lookup mechanism that first checks if any query environment exists in the current SPI context, and if so, delegates to the  function to perform the actual lookup. It includes an assertion to ensure the name parameter is not NULL, as any error would indicate a bug in the SPI implementation itself.

## Parameters / Member Variables
- : A C string containing the name of the ephemeral named relation to find

## Dependencies
- Functions called/Symbols referenced:
  - [get_ENR](../g/get_ENR.md) (for the actual ENR lookup)
  - Assert (for parameter validation)
- Called from (representative examples):
  - [SPI_register_relation](SPI_register_relation.md)
  - [SPI_unregister_relation](SPI_unregister_relation.md)

## Notes and Other Information
- This is an internal static function, not part of the public SPI API
- Includes fast-path optimization when no query environment exists (_SPI_current->queryEnv == NULL)
- Returns NULL if no matching ENR is found or if no query environment exists
- Uses assertions for parameter validation, indicating this is for internal use where NULL names should never occur
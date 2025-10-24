# btnametextcmp

## Location
[src/backend/utils/adt/varlena.c:2700-2715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2700-L2715)

## Overview
The  function implements a three-way comparison between a name type and a text type, returning an integer indicating their relative ordering.

## Definition

```c
Datum
btnametextcmp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs a collation-aware comparison between a name (fixed-length string) and a text value, returning -1 if the name is less than the text, 0 if they are equal, or +1 if the name is greater than the text. It serves as the foundation for various comparison operators by providing the core comparison logic. The function uses  to handle locale-specific sorting rules and character comparison.

## Parameters / Member Variables
- : Name type argument (extracted using )
- : Text type argument (extracted using )

## Dependencies
- Functions called/Symbols referenced:
  - : Extract name argument
  - : Extract text argument with possible detoasting
  - : Perform locale-aware string comparison
  - : Get collation for comparison
- Called from (representative examples):
  - : Less-than comparison (src/backend/utils/adt/varlena.c:2740)
  - : Less-than-or-equal comparison (src/backend/utils/adt/varlena.c:2746)
  - : Greater-than comparison (src/backend/utils/adt/varlena.c:2752)
  - : Greater-than-or-equal comparison (src/backend/utils/adt/varlena.c:2758)

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:2700-2715
- Returns int32 result: -1 (less than), 0 (equal), or +1 (greater than)
- Foundation function for all name-text ordering comparisons
- Uses collation-aware comparison through 
- Properly handles variable-length text data with detoasting
- Frees copied text argument to prevent memory leaks

## Simplified Source

```c
Datum btnametextcmp(PG_FUNCTION_ARGS) {
    // Extract arguments
    Name arg1 = PG_GETARG_NAME(0);
    text *arg2 = PG_GETARG_TEXT_PP(1);

    // Compare name string with text string using collation rules
    int32 result = varstr_cmp(NameStr(*arg1), strlen(NameStr(*arg1)),
                              VARDATA_ANY(arg2), VARSIZE_ANY_EXHDR(arg2),
                              PG_GET_COLLATION());

    // Clean up memory
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(result);
}
```
# bttextnamecmp

## Location
[src/backend/utils/adt/varlena.c:2716-2730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2716-L2730)

## Overview
The  function implements a three-way comparison between a text type and a name type, returning an integer indicating their relative ordering.

## Definition

```c
Datum
bttextnamecmp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs a collation-aware comparison between a text value and a name (fixed-length string), returning -1 if the text is less than the name, 0 if they are equal, or +1 if the text is greater than the name. It serves as the counterpart to  with reversed argument order and provides the foundation for text-to-name comparison operations. The function uses  to handle locale-specific sorting rules and character comparison.

## Parameters / Member Variables
- : Text type argument (extracted using )
- : Name type argument (extracted using )

## Dependencies
- Functions called/Symbols referenced:
  - : Extract text argument with possible detoasting
  - : Extract name argument
  - : Perform locale-aware string comparison
  - : Get collation for comparison
- Called from (representative examples):
  - : Less-than comparison (src/backend/utils/adt/varlena.c:2764)
  - : Less-than-or-equal comparison (src/backend/utils/adt/varlena.c:2770)
  - : Greater-than comparison (src/backend/utils/adt/varlena.c:2776)
  - : Greater-than-or-equal comparison (src/backend/utils/adt/varlena.c:2782)

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:2716-2730
- Returns int32 result: -1 (less than), 0 (equal), or +1 (greater than)
- Counterpart to  with reversed argument order
- Foundation function for all text-name ordering comparisons
- Uses collation-aware comparison through 
- Properly handles variable-length text data with detoasting
- Frees copied text argument to prevent memory leaks

## Simplified Source

```c
Datum
bttextnamecmp(PG_FUNCTION_ARGS)
{
    // Extract text and name arguments
    text *text_arg = PG_GETARG_TEXT_PP(0);
    Name name_arg = PG_GETARG_NAME(1);

    // Compare text with name using locale-aware comparison
    int32 result = varstr_cmp(VARDATA_ANY(text_arg), VARSIZE_ANY_EXHDR(text_arg),
                             NameStr(*name_arg), strlen(NameStr(*name_arg)),
                             PG_GET_COLLATION());

    // Clean up memory and return comparison result
    PG_FREE_IF_COPY(text_arg, 0);
    PG_RETURN_INT32(result);
}
```
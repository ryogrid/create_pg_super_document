# appendStringInfoText

## Location
[src/backend/utils/adt/xml.c:459-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L459-L466)

## Overview
Efficiently appends a PostgreSQL text value to a StringInfo buffer without converting to C string format.

## Definition

```c
static void
appendStringInfoText(StringInfo str, const text *t)
```
## Detailed Description
The appendStringInfoText function is an optimized utility for appending PostgreSQL text data types to StringInfo buffers. Unlike the more general appendStringInfoString function which requires converting text to a null-terminated C string first, this function directly extracts the binary data from the text value and appends it to the buffer.

This approach is more efficient because it avoids the overhead of text_to_cstring conversion and works directly with the variable-length text format used internally by PostgreSQL. The function uses VARDATA_ANY and VARSIZE_ANY_EXHDR macros to safely extract the data pointer and length from the text value.

## Parameters / Member Variables
- `str`: StringInfo buffer to append the text data to
- `t`: PostgreSQL text value to be appended

## Dependencies
- Functions called/Symbols referenced:
  - appendBinaryStringInfo (appends binary data to StringInfo)
  - VARDATA_ANY (macro to get data pointer from varlena)
  - VARSIZE_ANY_EXHDR (macro to get data size excluding header)
- Called from:
  - DatumGetVarStringPP (variable string processing)
  - [replace_text](../r/replace_text.md) (text replacement operations)
  - replace_text_regexp (regex text replacement)
  - [string_agg_transfn](../s/string_agg_transfn.md) (string aggregation function)
  - [xmlcomment](../x/xmlcomment.md) (XML comment generation)
  - [XmlTableGetValue](../X/XmlTableGetValue.md) (XML table value extraction)

## Notes and Other Information
- This is a static function, only available within the varlena.c compilation unit
- Provides better performance than appendStringInfoString for text values
- Handles both compressed and uncompressed text formats through VARDATA_ANY/VARSIZE_ANY_EXHDR
- Commonly used in string manipulation and XML processing functions
- The function does not null-terminate the appended data, as StringInfo handles this automatically
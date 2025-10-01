# makeJsonLexContext

## Location
[src/backend/utils/adt/jsonfuncs.c:538-565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L538-L565)

## Overview
This function creates a JsonLexContext for parsing JSON data from a PostgreSQL text datum, serving as a convenient wrapper around makeJsonLexContextCstringLen.

## Definition
JsonLexContext *makeJsonLexContext(JsonLexContext *lex, text *json, bool need_escapes)

## Detailed Description
makeJsonLexContext provides a simplified interface for creating JSON lexical contexts from PostgreSQL text values. It handles the common task of converting a text datum into the appropriate format for JSON parsing by automatically detoasting the input data and extracting the raw string content with proper length calculation. The function delegates the actual context creation to makeJsonLexContextCstringLen, passing the extracted string data, length, database encoding, and escape handling preferences. This abstraction shields callers from the complexities of PostgreSQL's variable-length data representation while ensuring proper memory management.

## Parameters / Member Variables
- `lex`: Existing JsonLexContext to potentially reuse or NULL to allocate a new one
- `json`: PostgreSQL text datum containing the JSON data to be parsed
- `need_escapes`: Boolean flag indicating whether escape sequence processing is required during parsing

## Dependencies
- Functions called/Symbols referenced:
  - [pg_detoast_datum_packed](../p/pg_detoast_datum_packed.md)
  - [makeJsonLexContextCstringLen](makeJsonLexContextCstringLen.md)
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
- Called from (representative examples):
  - [json_in](../j/json_in.md)
  - [json_validate](../j/json_validate.md)
  - [json_typeof](../j/json_typeof.md)
  - [json_object_keys](../j/json_object_keys.md)
  - [get_worker](../g/get_worker.md)

## Notes and Other Information
This function is fundamental to PostgreSQL's JSON processing pipeline as it bridges the gap between PostgreSQL's internal text representation and the JSON parser's string-based interface. The automatic detoasting ensures compatibility with both toasted and non-toasted text values, making it safe to use with data of any size. The function is widely used throughout the JSON function library as the standard entry point for text-to-JSON conversion operations.

## Simplified Source

```c
JsonLexContext *
makeJsonLexContext(JsonLexContext *lex, text *json, bool need_escapes)
{
    // Ensure input is detoasted for safe access
    json = pg_detoast_datum_packed(json);

    // Create lexical context with extracted string data
    return makeJsonLexContextCstringLen(lex,
                                        VARDATA_ANY(json),
                                        VARSIZE_ANY_EXHDR(json),
                                        GetDatabaseEncoding(),
                                        need_escapes);
}
```
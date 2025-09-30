# getJsonEncodingConst

## Location
[src/backend/parser/parse_expr.c:3227-3265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3227-L3265)

## Overview
Creates a Const node containing the JSON encoding name as a string, defaulting to UTF8 when no specific encoding is specified.

## Definition
```c
static Const *getJsonEncodingConst(JsonFormat *format)
```

## Detailed Description
This static helper function converts a JSON format specification into a constant node containing the appropriate encoding name string. It handles the mapping from internal JsonEncoding enumeration values to their corresponding string representations ("UTF8", "UTF16", "UTF32"). The function implements the default behavior where UTF8 encoding is used when no format is provided, when the format type is default, or when the encoding is explicitly set to default.

The function creates a Name-type constant node that can be used in JSON-related function calls or expressions. It allocates memory for the name data structure and uses PostgreSQL's standard constant creation facilities to produce a properly formed Const node.

## Parameters / Member Variables
- `format`: Pointer to JsonFormat structure containing encoding specifications (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [namestrcpy](../n/namestrcpy.md) (copy string to Name structure)  
  - [makeConst](../m/makeConst.md) (create Const node)
  - [NameGetDatum](../N/NameGetDatum.md) (convert Name to Datum)
  - elog (error logging)
  - [JsonFormat](../J/JsonFormat.md) (input structure type)
  - JsonEncoding (enumeration type)
  - Name, NameData (name data types)
  - JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, JS_ENC_UTF8, JS_ENC_UTF16, JS_ENC_UTF32 (enumeration constants)
  - NAMEOID, InvalidOid, NAMEDATALEN (PostgreSQL constants)
- Called from (representative examples):
  - [makeJsonByteaToTextConversion](../m/makeJsonByteaToTextConversion.md)
  - [coerceJsonFuncExpr](../c/coerceJsonFuncExpr.md)

## Notes and Other Information
- This is a static helper function within parse_expr.c
- UTF8 is used as the default encoding in multiple scenarios
- The function performs error checking for invalid encoding values
- Memory is allocated for the name structure using palloc
- The resulting Const node has type NAMEOID and uses standard name data format
- Used in JSON processing functionality for encoding specification

## Simplified Source

```c
static Const *getJsonEncodingConst(JsonFormat *format) {
    JsonEncoding encoding;
    const char *enc;
    Name encname = palloc(sizeof(NameData));

    // Default to UTF8 if no format or default format/encoding specified
    if (!format ||
        format->format_type == JS_FORMAT_DEFAULT ||
        format->encoding == JS_ENC_DEFAULT) {
        encoding = JS_ENC_UTF8;
    } else {
        encoding = format->encoding;
    }

    // Map encoding enum to string representation
    switch (encoding) {
        case JS_ENC_UTF16:
            enc = "UTF16";
            break;
        case JS_ENC_UTF32:
            enc = "UTF32";
            break;
        case JS_ENC_UTF8:
            enc = "UTF8";
            break;
        default:
            elog(ERROR, "invalid JSON encoding: %d", encoding);
            break;
    }

    // Copy encoding string to Name structure
    namestrcpy(encname, enc);

    // Create and return Const node with encoding name
    return makeConst(NAMEOID, -1, InvalidOid, NAMEDATALEN,
                     NameGetDatum(encname), false, false);
}
```
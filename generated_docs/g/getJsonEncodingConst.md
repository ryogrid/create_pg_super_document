# getJsonEncodingConst

## Location
src/backend/parser/parse_expr.c: 3227 - 3265

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
  - namestrcpy (copy string to Name structure)  
  - [makeConst](../m/makeConst.md) (create Const node)
  - [NameGetDatum](../N/NameGetDatum.md) (convert Name to Datum)
  - elog (error logging)
  - JsonFormat (input structure type)
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
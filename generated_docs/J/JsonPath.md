# JsonPath

## Location
[src/include/utils/jsonpath.h:28-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonpath.h#L28-L29)

## Overview
JsonPath is a struct that represents the PostgreSQL jsonpath datatype in binary format, used for querying and navigating JSON data structures using JSONPath expressions.

## Definition
```c
typedef struct
{
    int32   vl_len_;        /* varlena header (do not touch directly!) */
    uint32  header;         /* version and flags (see below) */
    char    data[FLEXIBLE_ARRAY_MEMBER];
} JsonPath;
```

## Detailed Description
The JsonPath structure is the core data type for PostgreSQL's JSONPath implementation, which provides SQL/JSON path language support. This struct stores a compiled jsonpath expression in binary format, allowing efficient execution of JSONPath queries against JSON and JSONB data.

The structure follows PostgreSQL's varlena format, making it a variable-length data type that can be stored in the database. The binary representation includes:
- A standard varlena header for length management
- A header field containing version information and execution mode flags (particularly the LAX vs STRICT mode flag)  
- A flexible array member containing the compiled JSONPath expression data

The jsonpath expressions support the full SQL/JSON path language specification including property access (.key), array indexing ([n]), recursive descent (..**), filters (?(predicate)), and various built-in methods (.type(), .size(), etc.).

## Parameters / Member Variables
- `vl_len_`: Standard PostgreSQL varlena header containing the total length of the structure (including header and data). Should not be accessed directly - use VARSIZE() macro instead.
- `header`: Contains version and mode flags. Uses JSONPATH_VERSION for version identification and JSONPATH_LAX flag to indicate LAX mode execution (vs strict mode).
- `data`: Flexible array member containing the serialized binary representation of the parsed JSONPath expression tree.

## Dependencies
- Functions called/Symbols referenced:
  - No direct symbol references (this is a pure data structure)
- Called from (representative examples):
  - [jsonpath_out](../j/jsonpath_out.md)
  - [jsonpath_send](../j/jsonpath_send.md)  
  - [jspInit](../j/jspInit.md)
  - [JsonPathExists](JsonPathExists.md)
  - [JsonPathQuery](JsonPathQuery.md)
  - [JsonPathValue](JsonPathValue.md)
  - [DatumGetJsonPathP](../D/DatumGetJsonPathP.md)
  - [DatumGetJsonPathPCopy](../D/DatumGetJsonPathPCopy.md)

## Notes and Other Information
- The JSONPATH_HDRSZ macro defines the header size as offsetof(JsonPath, data)
- The JSONPATH_VERSION constant (0x01) identifies the current binary format version
- The JSONPATH_LAX flag (0x80000000) in the header indicates LAX mode execution, where path errors are ignored
- Helper macros PG_GETARG_JSONPATH_P() and PG_RETURN_JSONPATH_P() are provided for function argument handling
- The struct is designed to be stored efficiently in PostgreSQL's TOAST system for large jsonpath expressions
- Binary format is upgrade-safe and maintains compatibility across PostgreSQL versions
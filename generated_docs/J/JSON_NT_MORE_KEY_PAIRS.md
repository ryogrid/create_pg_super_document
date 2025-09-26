# JSON_NT_MORE_KEY_PAIRS

## Location
[src/common/jsonapi.c:59-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L59-L61)

## Overview
An enumeration value in the JsonNonTerminal enum that represents additional key-value pairs in JSON objects during incremental JSON parsing.

## Definition
```c
enum JsonNonTerminal
{
    JSON_NT_JSON = 32,
    JSON_NT_ARRAY_ELEMENTS,
    JSON_NT_MORE_ARRAY_ELEMENTS,
    JSON_NT_KEY_PAIRS,
    JSON_NT_MORE_KEY_PAIRS,
};
```

## Detailed Description
JSON_NT_MORE_KEY_PAIRS is a non-terminal symbol used in PostgreSQL's incremental JSON parser state machine. It represents the grammar production for parsing additional key-value pairs that follow the first key-value pair in a JSON object. This enum value is part of the parser's internal representation for handling the comma-separated sequence of key-value pairs within JSON objects (e.g., in `{"key1": "value1", "key2": "value2"}`), specifically handling the `, "key2": "value2"` portion.

The value is used by the JSON parser's state machine to track parsing progress and determine appropriate semantic actions during incremental JSON processing.

## Parameters / Member Variables
- This is an enum constant with no parameters or member variables

## Dependencies
- Functions called/Symbols referenced:
  - None (enum constant)
- Called from (representative examples):
  - IS_NT (macro usage in parsing logic)
  - TD_ENTRY (macro usage in transition table)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (main parsing function)

## Notes and Other Information
- Part of the JsonNonTerminal enum starting at value 32
- Used in conjunction with JSON_NT_KEY_PAIRS to handle complete key-value pair sequences in JSON objects
- Essential for the incremental JSON parser's ability to handle arbitrarily long sequences of object properties
- The parser uses this to differentiate between the first key-value pair and subsequent ones in an object
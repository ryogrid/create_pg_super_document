# td_entry

## Location
src/common/jsonapi.c: 176 - 177

## Overview
A typedef for a structure that represents entries in the JSON parser's production table, connecting grammar productions with their director sets of terminal symbols.

## Definition
```c
typedef struct
{
    size_t      len;
    char       *prod;
} td_entry;
```

## Detailed Description
The td_entry structure is used to define entries in PostgreSQL's incremental JSON parser table (td_parser_table). Each entry represents a grammar production rule that can be applied when specific terminal symbols are encountered during parsing. The structure stores both the length and content of production rules, which are used by the parser's state machine to determine the appropriate parsing actions.

The parser table uses td_entry structures to map combinations of non-terminal and terminal symbols to specific production rules. Any combination not specified in the table represents a parsing error. This approach allows the parser to efficiently determine which grammar production to apply based on the current parsing context and input tokens.

## Parameters / Member Variables
- `len`: Size of the production string in bytes (excluding null terminator)
- `prod`: Pointer to the production string that defines the grammar rule

## Dependencies
- Functions called/Symbols referenced:
  - None (structure definition)
- Called from (representative examples):
  - TD_ENTRY (macro for creating table entries)
  - push_prediction (function that uses td_entry for parser predictions)
  - pg_parse_json_incremental (main parsing function that accesses the parser table)

## Notes and Other Information
- Used in conjunction with the TD_ENTRY macro which simplifies creation of table entries by automatically calculating string length
- The td_parser_table is a 2D array indexed by non-terminal and terminal symbols, with td_entry values
- Essential component of the LR-style parsing table that drives the incremental JSON parser
- The len field optimization avoids repeated strlen() calls during parsing
- Production strings referenced by prod field are typically compile-time string constants
# store_input_from_desc

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1159-1212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1159-L1212)

## Overview
Converts data from a descriptor item into a format suitable for parameter insertion in SQL statements, handling both binary and text data.

## Definition

```c
static bool
store_input_from_desc(struct statement *stmt, struct descriptor_item *desc_item,
					  char **tobeinserted)
```
## Detailed Description
This function serves as an adapter between SQL descriptor items and the ECPG parameter system. It handles two distinct data types: binary data (which is copied directly) and text data (which requires conversion through the variable system). For text data, it constructs a temporary variable structure with appropriate type information and indicator handling, then uses the standard ecpg_store_input mechanism for consistent processing and formatting.

## Parameters / Member Variables
- `*stmt`: Pointer to the statement structure containing execution context
- `*desc_item`: Pointer to the descriptor item containing the source data and metadata
- `**tobeinserted`: Output parameter that receives the allocated and formatted data string
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - ECPGt_char
  - [ecpg_store_input](../e/ecpg_store_input.md)
- Called from:
  - [ecpg_build_params](../e/ecpg_build_params.md)

## Notes and Other Information
- Returns true on successful conversion, false on allocation or processing failure
- Binary data is handled through direct memory allocation and copying for efficiency
- Text data goes through the full variable conversion system with proper indicator support
- The function properly sets up variable structures with all required fields for ecpg_store_input
- Memory allocation failures are properly handled and propagated to caller
- Critical component in SQL descriptor-based parameter processing within ECPG
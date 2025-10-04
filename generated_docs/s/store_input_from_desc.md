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

## Simplified Source

```c
static bool
store_input_from_desc(struct statement *stmt, struct descriptor_item *desc_item,
                      char **tobeinserted)
{
    // Binary data: direct memory copy
    if (desc_item->is_binary)
    {
        *tobeinserted = ecpg_alloc(desc_item->data_len, stmt->lineno);
        if (!*tobeinserted) return false;
        memcpy(*tobeinserted, desc_item->data, desc_item->data_len);
        return true;
    }

    // Text data: setup variable structure for conversion
    struct variable var;
    var.type = ECPGt_char;
    var.varcharsize = strlen(desc_item->data);
    var.value = desc_item->data;
    var.pointer = &(desc_item->data);
    var.arrsize = 1;
    var.offset = 0;

    // Setup indicator information
    if (!desc_item->indicator)
    {
        var.ind_type = ECPGt_NO_INDICATOR;
        var.ind_value = var.ind_pointer = NULL;
        var.ind_varcharsize = var.ind_arrsize = var.ind_offset = 0;
    }
    else
    {
        var.ind_type = ECPGt_int;
        var.ind_value = &(desc_item->indicator);
        var.ind_pointer = &(var.ind_value);
        var.ind_varcharsize = var.ind_arrsize = 1;
        var.ind_offset = 0;
    }

    // Convert through standard input processing
    return ecpg_store_input(stmt->lineno, stmt->force_indicator, &var,
                           tobeinserted, false);
}
```
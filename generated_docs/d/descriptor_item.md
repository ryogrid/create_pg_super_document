# descriptor_item

## Location
[src/interfaces/ecpg/ecpglib/ecpglib_extern.h:124-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/ecpglib_extern.h#L124-L137)

## Overview
The `descriptor_item` struct represents individual items (columns or parameters) within an SQL descriptor, storing detailed metadata about each data element in ECPG operations.

## Definition
```c
struct descriptor_item
{
    int         num;
    char       *data;
    int         indicator;
    int         length;
    int         precision;
    int         scale;
    int         type;
    bool        is_binary;
    int         data_len;
    struct descriptor_item *next;
};
```

## Detailed Description
This structure contains comprehensive metadata for individual columns in a result set or parameters in a prepared statement. It serves as the building block for SQL descriptors, providing all necessary information for data type handling, memory management, and data conversion operations. The structure forms a linked list to represent multiple items within a single descriptor, supporting the full range of SQL data types and their characteristics.

## Parameters / Member Variables
- `num`: Sequential number or position identifier for this descriptor item
- `data`: Pointer to the actual data value for this item, formatted according to its type
- `indicator`: SQL indicator value (negative for NULL, zero for normal values, positive for special conditions)
- `length`: Maximum length or size of the data item in bytes
- `precision`: Numeric precision for decimal and floating-point types
- `scale`: Numeric scale (decimal places) for decimal types
- `type`: PostgreSQL internal type identifier indicating the data type of this item
- `is_binary`: Boolean flag indicating whether the data is stored in binary format
- `data_len`: Actual length of the data currently stored (may be less than maximum length)
- `next`: Pointer to the next descriptor item in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - descriptor_item (self-reference for linked list)
- Called from (representative examples):
  - set_desc_attr (setting descriptor attributes)
  - ECPGset_desc (descriptor manipulation)
  - descriptor_free (cleanup operations)
  - store_input_from_desc (parameter handling)
  - ecpg_build_params (parameter building)

## Notes and Other Information
This structure is fundamental to ECPGs dynamic SQL capabilities, enabling runtime introspection and manipulation of SQL data structures. The comprehensive metadata allows for proper data conversion, memory management, and NULL handling across different PostgreSQL data types. The linked list design supports variable-length result sets and parameter lists, which is essential for dynamic SQL operations where the structure is not known at compile time.
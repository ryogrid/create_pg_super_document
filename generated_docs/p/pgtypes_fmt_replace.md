# pgtypes_fmt_replace

## Location
[src/interfaces/ecpg/pgtypeslib/common.c:30-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/common.c#L30-L144)

## Overview
A comprehensive format replacement function in the PostgreSQL ECPG pgtypeslib that handles the conversion and formatting of various data types into string representations for output formatting.

## Definition
```c
int pgtypes_fmt_replace(union un_fmt_comb replace_val, int replace_type, char **output, int *pstr_len)
```

## Detailed Description
`pgtypes_fmt_replace` is a central formatting function that handles the conversion of various PostgreSQL data types into their string representations. The function supports multiple data types including strings (both constant and mallocd), characters, doubles, and various integer types with different formatting options (zero-padded, left-padded, etc.).

The function operates by examining the `replace_type` parameter to determine how to process the input value, then formats the value appropriately and writes it to the output buffer. It manages memory allocation internally for numeric conversions and ensures proper buffer bounds checking to prevent overflows.

Key features include:
- Support for multiple data type conversions
- Memory management for dynamically allocated strings
- Buffer overflow protection
- Consistent error handling with return codes
- Various numeric formatting options (zero-padding, field width control)

## Parameters / Member Variables
- `replace_val`: A union containing the value to be formatted, with the actual member used determined by `replace_type`
- `replace_type`: An integer constant specifying the type of data and formatting to apply
- `output`: A pointer to a char pointer that points to the current position in the output buffer; updated to point past the written data
- `pstr_len`: A pointer to an integer containing the remaining space in the output buffer; updated to reflect remaining space after writing

## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_alloc](pgtypes_alloc.md) (for temporary buffer allocation)
  - `strlen` (for string length calculation)
  - `memcpy` (for string copying)
  - `free` (for memory deallocation)
  - `snprintf` (for numeric formatting)
  - `strcpy` (for string copying)
  - Various PGTYPES constants and macros:
    - `PGTYPES_TYPE_NOTHING`
    - `PGTYPES_TYPE_STRING_CONSTANT`
    - `PGTYPES_TYPE_STRING_MALLOCED`
    - `PGTYPES_TYPE_CHAR`
    - `PGTYPES_TYPE_DOUBLE_NF`
    - `PGTYPES_TYPE_INT64`
    - `PGTYPES_TYPE_UINT`
    - `PGTYPES_TYPE_UINT_2_LZ`
    - `PGTYPES_TYPE_UINT_2_LS`
    - `PGTYPES_TYPE_UINT_3_LZ`
    - `PGTYPES_TYPE_UINT_4_LZ`
    - `PGTYPES_FMT_NUM_MAX_DIGITS`
    - `INT64_FORMAT`
- Called from (representative examples):
  - `un_fmt_comb`
  - [dttofmtasc_replace](../d/dttofmtasc_replace.md)

## Notes and Other Information
- Returns 0 on success, -1 on buffer overflow or other formatting errors, or ENOMEM on memory allocation failure
- Automatically frees mallocd strings after copying them to the output buffer
- Performs bounds checking to prevent buffer overflows
- Handles various numeric formatting options including zero-padding and field width specifications
- The function updates both `output` and `pstr_len` parameters to reflect the new buffer state after writing
- Critical component of the PostgreSQL ECPG formatting system for date, time, and numeric data types
- Located in `src/interfaces/ecpg/pgtypeslib/common.c:30-144`

## Simplified Source

```c
int pgtypes_fmt_replace(union un_fmt_comb replace_val, int replace_type, char **output, int *pstr_len)
{
    int i = 0;

    switch (replace_type) {
        case PGTYPES_TYPE_NOTHING:
            break;

        case PGTYPES_TYPE_STRING_CONSTANT:
        case PGTYPES_TYPE_STRING_MALLOCED:
            // Copy string to output buffer
            i = strlen(replace_val.str_val);
            if (i + 1 <= *pstr_len) {
                memcpy(*output, replace_val.str_val, i + 1);
                *pstr_len -= i;
                *output += i;
                if (replace_type == PGTYPES_TYPE_STRING_MALLOCED)
                    free(replace_val.str_val);
                return 0;
            }
            return -1;

        case PGTYPES_TYPE_CHAR:
            // Copy single character
            if (*pstr_len >= 2) {
                (*output)[0] = replace_val.char_val;
                (*output)[1] = '\0';
                (*pstr_len)--;
                (*output)++;
                return 0;
            }
            return -1;

        case PGTYPES_TYPE_DOUBLE_NF:
        case PGTYPES_TYPE_INT64:
        case PGTYPES_TYPE_UINT:
        case PGTYPES_TYPE_UINT_2_LZ:
        case PGTYPES_TYPE_UINT_2_LS:
        case PGTYPES_TYPE_UINT_3_LZ:
        case PGTYPES_TYPE_UINT_4_LZ:
            {
                // Format numeric values with appropriate format specifiers
                char *temp_buffer = pgtypes_alloc(PGTYPES_FMT_NUM_MAX_DIGITS);
                if (!temp_buffer)
                    return ENOMEM;

                // Format based on type
                switch (replace_type) {
                    case PGTYPES_TYPE_DOUBLE_NF:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, "%0.0g", replace_val.double_val);
                        break;
                    case PGTYPES_TYPE_INT64:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, INT64_FORMAT, replace_val.int64_val);
                        break;
                    case PGTYPES_TYPE_UINT:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, "%u", replace_val.uint_val);
                        break;
                    case PGTYPES_TYPE_UINT_2_LZ:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, "%02u", replace_val.uint_val);
                        break;
                    case PGTYPES_TYPE_UINT_2_LS:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, "%2u", replace_val.uint_val);
                        break;
                    case PGTYPES_TYPE_UINT_3_LZ:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, "%03u", replace_val.uint_val);
                        break;
                    case PGTYPES_TYPE_UINT_4_LZ:
                        i = snprintf(temp_buffer, PGTYPES_FMT_NUM_MAX_DIGITS, "%04u", replace_val.uint_val);
                        break;
                }

                // Check buffer space and copy result
                if (i < 0 || i >= PGTYPES_FMT_NUM_MAX_DIGITS || *pstr_len <= strlen(temp_buffer)) {
                    free(temp_buffer);
                    return -1;
                }

                strcpy(*output, temp_buffer);
                *output += strlen(temp_buffer);
                *pstr_len -= strlen(temp_buffer);
                free(temp_buffer);
            }
            break;
    }
    return 0;
}
```
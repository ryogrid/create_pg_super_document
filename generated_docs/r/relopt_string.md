# relopt_string

## Location
src/include/access/reloptions.h: 139 - 147

## Overview
A structure that defines a string-type relation option in PostgreSQL's relation options system, including validation callbacks and default values for string parameters.

## Definition
```c
typedef struct relopt_string
{
    relopt_gen  gen;
    int         default_len;
    bool        default_isnull;
    validate_string_relopt validate_cb;
    fill_string_relopt fill_cb;
    char       *default_val;
} relopt_string;
```

## Detailed Description
The `relopt_string` structure represents a string-valued relation option definition. It extends the base `relopt_gen` structure and adds string-specific functionality including default value handling, length tracking, null state management, and validation/filling callbacks. This structure enables PostgreSQL to handle relation options that accept arbitrary string values with custom validation logic, such as file paths, custom parameter strings, or encoded configuration data.

## Parameters / Member Variables
- `gen`: Base relation option metadata inherited from relopt_gen structure
- `default_len`: Length of the default string value, used for memory allocation and validation
- `default_isnull`: Boolean flag indicating whether the default value is NULL
- `validate_cb`: Callback function pointer for custom validation of string values
- `fill_cb`: Callback function pointer for custom processing when filling option values
- `default_val`: Pointer to the default string value to use when the option is not specified

## Dependencies
- Functions called/Symbols referenced:
  - relopt_gen (Line 141)
- Called from (representative examples):
  - GET_STRING_RELOPTION_LEN (src/backend/access/common/reloptions.c:571)
  - allocate_reloption (src/backend/access/common/reloptions.c:802)
  - init_string_reloption (src/backend/access/common/reloptions.c:1059, 1065)
  - add_string_reloption (src/backend/access/common/reloptions.c:1102)
  - add_local_string_reloption (src/backend/access/common/reloptions.c:1123)
  - parse_one_reloption (src/backend/access/common/reloptions.c:1682)
  - fillRelOptions (src/backend/access/common/reloptions.c:1768, 1795)

## Notes and Other Information
The callback functions (validate_cb and fill_cb) provide extensibility for custom string processing. The default_len field is crucial for proper memory management and bounds checking. The default_isnull flag allows distinguishing between empty strings and NULL values, which is important for proper SQL semantics.
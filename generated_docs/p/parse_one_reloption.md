# parse_one_reloption

## Location
[src/backend/access/common/reloptions.c:1578-1710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1578-L1710)

## Overview
Static function that parses and validates a single relation option's value, handling type-specific parsing and validation for different option types (bool, int, real, enum, string).

## Definition
```c
static void parse_one_reloption(relopt_value *option, char *text_str, int text_len, bool validate)
```

## Detailed Description
This function is the core parsing logic for individual relation options, called by parseRelOptionsInternal() for each "key=value" pair. It extracts the value portion from the text string, performs type-specific parsing based on the option's defined type (RELOPT_TYPE_BOOL, RELOPT_TYPE_INT, RELOPT_TYPE_REAL, RELOPT_TYPE_ENUM, RELOPT_TYPE_STRING), and validates the parsed value against type-specific constraints. The function handles duplicate option detection, range checking for numeric types, enum value validation, and optional string validation callbacks. Upon successful parsing, it marks the option as set (isset=true) and stores the parsed value in the appropriate union member.

## Parameters / Member Variables
- `option`: Pointer to relopt_value structure to populate with parsed value
- `text_str`: Raw text string containing "optionname=value" format
- `text_len`: Length of the text_str parameter
- `validate`: Boolean flag enabling validation and error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [palloc](palloc.md)
  - memcpy
  - [parse_bool](parse_bool.md)
  - [parse_int](parse_int.md)
  - [parse_real](parse_real.md)
  - [pg_strcasecmp](pg_strcasecmp.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - elog
  - [pfree](pfree.md)
- Called from (representative examples):
  - [parseRelOptionsInternal](parseRelOptionsInternal.md)

## Notes and Other Information
- Detects and reports duplicate option specifications when validation is enabled
- Performs bounds checking for integer and real number types against min/max constraints
- Supports case-insensitive enum value matching using pg_strcasecmp
- [String](../S/String.md) values are stored directly without copying (nofree=true) for efficiency
- Provides detailed error messages including valid value ranges for out-of-bounds errors
- Uses PostgreSQL's standard error reporting system with appropriate error codes
- Memory management includes conditional freeing based on option type (strings are not freed)

## Simplified Source

```c
static void parse_one_reloption(relopt_value *option, char *text_str, int text_len, bool validate) {
    // Check for duplicate option
    if (option->isset && validate)
        ereport(ERROR, "parameter specified more than once");

    // Extract value string from "name=value" format
    value_len = text_len - option->gen->namelen - 1;
    value = palloc(value_len + 1);
    memcpy(value, text_str + option->gen->namelen + 1, value_len);
    value[value_len] = '\0';

    // Parse based on option type
    switch (option->gen->type) {
        case RELOPT_TYPE_BOOL:
            parsed = parse_bool(value, &option->values.bool_val);
            break;

        case RELOPT_TYPE_INT:
            parsed = parse_int(value, &option->values.int_val, 0, NULL);
            // Validate range if specified
            if (validate && parsed && out_of_bounds)
                ereport(ERROR, "value out of bounds");
            break;

        case RELOPT_TYPE_REAL:
            parsed = parse_real(value, &option->values.real_val, 0, NULL);
            // Validate range if specified
            break;

        case RELOPT_TYPE_ENUM:
            // Search enum members for matching value
            for (elt = optenum->members; elt->string_val; elt++) {
                if (pg_strcasecmp(value, elt->string_val) == 0) {
                    option->values.enum_val = elt->symbol_val;
                    parsed = true;
                    break;
                }
            }
            break;

        case RELOPT_TYPE_STRING:
            option->values.string_val = value;
            nofree = true;  // Don't free string values
            parsed = true;
            break;
    }

    if (parsed)
        option->isset = true;
    if (!nofree)
        pfree(value);
}
```
# valid_custom_variable_name

## Location
[src/backend/utils/misc/guc.c:1078-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1078-L1122)

## Overview
Validates whether a proposed custom variable name follows PostgreSQL's naming rules for GUC configuration parameters.

## Definition
```c
static bool valid_custom_variable_name(const char *name)
```

## Detailed Description
This function enforces PostgreSQL's naming convention for custom GUC variables, which requires them to follow a specific pattern: two or more identifiers separated by dots (periods). The validation rules align with PostgreSQL's lexical scanner (scan.l) to ensure consistency across the system.

The function performs character-by-character validation with the following rules:
1. **Separator requirement**: The name must contain at least one GUC_QUALIFIER_SEPARATOR (dot) to distinguish custom variables from built-in ones
2. **Identifier rules**: Each component separated by dots must be a valid identifier
3. **First character rules**: Identifiers can start with letters (A-Z, a-z), underscore (_), or high-bit characters
4. **Non-first character rules**: After the first character, identifiers can also contain digits (0-9) and dollar signs ($)
5. **Empty components**: Empty name components (consecutive dots or leading/trailing dots) are not allowed

## Parameters / Member Variables
- `name`: The proposed custom variable name to validate

## Dependencies
- Functions called/Symbols referenced:
  - strchr
  - GUC_QUALIFIER_SEPARATOR
  - IS_HIGHBIT_SET
- Called from:
  - [assignable_custom_variable_name](../a/assignable_custom_variable_name.md) (src/backend/utils/misc/guc.c:1134)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md) (src/backend/utils/misc/guc.c:4733)
  - [validate_option_array_item](validate_option_array_item.md) (src/backend/utils/misc/guc.c:6746)

## Notes and Other Information
- This is a static function, only accessible within the guc.c file
- Returns true if the name is valid for a custom variable, false otherwise
- The function ensures custom variables are always namespaced (contain at least one dot)
- Character validation rules match PostgreSQL's lexical scanner to maintain consistency
- High-bit characters are allowed to support international character sets
- The validation helps prevent conflicts between custom variables and built-in PostgreSQL parameters
- If this validation rule changes, the error detail in assignable_custom_variable_name() should also be updated accordingly

## Simplified Source

```c
static bool valid_custom_variable_name(const char *name)
{
    bool saw_separator = false;
    bool at_name_start = true;

    // Iterate through each character in the name
    for (const char *p = name; *p; p++)
    {
        if (*p == GUC_QUALIFIER_SEPARATOR)  // dot separator
        {
            if (at_name_start)
                return false;  // empty component (e.g., "..name" or ".name")
            saw_separator = true;
            at_name_start = true;
        }
        else if (strchr("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_", *p) ||
                 IS_HIGHBIT_SET(*p))
        {
            // Valid first or continuation character
            at_name_start = false;
        }
        else if (!at_name_start && strchr("0123456789$", *p))
        {
            // Valid continuation character only
        }
        else
            return false;  // Invalid character
    }

    if (at_name_start)
        return false;  // empty final component

    // Valid only if we found at least one separator
    return saw_separator;
}
```
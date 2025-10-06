# numeric_normalize

## Location
[src/backend/utils/adt/numeric.c:1024-1075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1024-L1075)

## Overview
This function converts a PostgreSQL Numeric value to a normalized string representation by removing trailing zeros and unnecessary decimal points to ensure that equal numeric values produce identical string representations.

## Definition

```c
char *
numeric_normalize(Numeric num)
```
## Detailed Description
The  function produces a canonical string representation of PostgreSQL's Numeric data type. Its primary purpose is to ensure that numerically equal values result in identical string representations by suppressing insignificant trailing zeros and removing trailing decimal points when they become unnecessary. This normalization is crucial for operations that require string-based comparisons of numeric values to be consistent with numeric equality comparisons. The function handles special values (NaN, ±Infinity) by returning standard string literals, and for regular numeric values, it converts to string form and then performs trailing zero removal.

## Parameters / Member Variables
- `num`: The input Numeric value to be normalized to canonical string form
## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL
  - NUMERIC_IS_PINF  
  - NUMERIC_IS_NINF
  - [pstrdup](../p/pstrdup.md)
  - [init_var_from_num](../i/init_var_from_num.md)
  - [get_str_from_var](../g/get_str_from_var.md)
  - strchr
  - strlen
- Called from (representative examples):
  - [make_scalar_key](../m/make_scalar_key.md) (src/backend/utils/adt/jsonb_gin.c:1393)

## Notes and Other Information
- The function ensures that equal numeric values produce identical normalized strings
- Removes trailing zeros from fractional parts and eliminates unnecessary decimal points
- Returns a dynamically allocated string that must be freed by the caller
- Particularly important for GIN indexing and JSON operations where string representation consistency matters
- Located in src/backend/utils/adt/numeric.c:1024-1075
- The normalization algorithm works backwards from the end of the string to efficiently remove trailing zeros

## Simplified Source

```c
char *numeric_normalize(Numeric num) {
    NumericVar x;
    char *str;
    int last;

    // Handle special values
    if (NUMERIC_IS_SPECIAL(num)) {
        if (NUMERIC_IS_PINF(num))
            return pstrdup("Infinity");
        else if (NUMERIC_IS_NINF(num))
            return pstrdup("-Infinity");
        else
            return pstrdup("NaN");
    }

    // Convert to internal representation and get string
    init_var_from_num(num, &x);
    str = get_str_from_var(&x);

    // Remove trailing zeros and decimal point if needed
    if (strchr(str, '.') != NULL) {
        // Find last non-zero character
        last = strlen(str) - 1;
        while (str[last] == '0')
            last--;

        // Remove decimal point if it's now the last character
        if (str[last] == '.')
            last--;

        // Truncate string at the new end
        str[last + 1] = '\0';
    }

    return str;
}
```
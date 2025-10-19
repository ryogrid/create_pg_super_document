# makeVariableValue

## Location
[src/bin/pgbench/pgbench.c:1664-1737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1664-L1737)

## Overview
Converts a variable's string value to its appropriate typed representation (null, boolean, integer, or double) based on content analysis.

## Definition

```c
static bool
makeVariableValue(Variable *var)
```
## Detailed Description
The  function performs type inference and conversion on a variable that currently only has a string representation. It analyzes the string content to determine the most appropriate data type and converts the value accordingly. The function supports conversion to NULL, boolean (with flexible true/false representations), 64-bit integers, and double-precision floating point numbers. If conversion fails due to malformed input, it returns false and logs an error message. This function is essential for pgbench's dynamic typing system where variables can be assigned as strings but need to be used as typed values in expressions.

## Parameters / Member Variables
- `*var`: Pointer to the Variable structure to convert
## Dependencies
- Functions called/Symbols referenced:
  - strlen
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [setNullValue](../s/setNullValue.md)
  - [setBoolValue](../s/setBoolValue.md)
  - [is_an_int](../i/is_an_int.md)
  - [strtoint64](../s/strtoint64.md)
  - [setIntValue](../s/setIntValue.md)
  - [strtodouble](../s/strtodouble.md)
  - [setDoubleValue](../s/setDoubleValue.md)
  - pg_log_error
- Types referenced:
  - [Variable](../V/Variable.md)
  - PGBT_NO_VALUE
  - int64
- Called from (representative examples):
  - [evaluateExpr](../e/evaluateExpr.md)

## Notes and Other Information
- Returns true on successful conversion, false on failure
- Supports flexible boolean parsing (accepts prefixes like 'y', 'ye', 'n', 'no', but not 'o')
- Recognizes 'on'/'off' and 'of' as boolean values
- Empty strings cause conversion failure
- [Integer](../I/Integer.md) overflow during conversion results in failure
- Double conversion allows for scientific notation and handles malformed input gracefully
- Part of pgbench's expression evaluation system for dynamic typing

## Simplified Source

```c
static bool makeVariableValue(Variable *var) {
    // Convert string variable to appropriate typed value

    if (var->value.type != PGBT_NO_VALUE) {
        return true;  // Already converted
    }

    size_t slen = strlen(var->svalue);
    if (slen == 0) return false;  // Empty strings not allowed

    // Check for NULL value
    if (pg_strcasecmp(var->svalue, "null") == 0) {
        setNullValue(&var->value);
    }
    // Check for boolean TRUE values (allows prefixes: y, ye, yes, t, tr, tru, true)
    else if (pg_strncasecmp(var->svalue, "true", slen) == 0 ||
             pg_strncasecmp(var->svalue, "yes", slen) == 0 ||
             pg_strcasecmp(var->svalue, "on") == 0) {
        setBoolValue(&var->value, true);
    }
    // Check for boolean FALSE values (allows prefixes: n, no, f, fa, fal, fals, false)
    else if (pg_strncasecmp(var->svalue, "false", slen) == 0 ||
             pg_strncasecmp(var->svalue, "no", slen) == 0 ||
             pg_strcasecmp(var->svalue, "off") == 0 ||
             pg_strcasecmp(var->svalue, "of") == 0) {
        setBoolValue(&var->value, false);
    }
    // Check for integer values
    else if (is_an_int(var->svalue)) {
        int64 iv;
        if (!strtoint64(var->svalue, false, &iv)) {
            return false;  // Integer overflow
        }
        setIntValue(&var->value, iv);
    }
    // Default to double conversion
    else {
        double dv;
        if (!strtodouble(var->svalue, true, &dv)) {
            pg_log_error("malformed variable \"%s\" value: \"%s\"",
                        var->name, var->svalue);
            return false;
        }
        setDoubleValue(&var->value, dv);
    }

    return true;
}
```

**Key Points:**
- Performs type inference: null → boolean → integer → double (in that order)
- Supports flexible boolean parsing with prefixes and common aliases
- Returns false for empty strings or conversion failures
- Essential for pgbench's dynamic typing system in expressions
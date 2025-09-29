# printTypmod

## Location
[src/backend/utils/adt/format_type.c:371-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/format_type.c#L371-L411)

## Overview
A static helper function that adds type modifier (typmod) decoration to a PostgreSQL type name, formatting it into a human-readable string representation.

## Definition

```c
static char *
printTypmod(const char *typname, int32 typmod, Oid typmodout)
```
## Detailed Description
The  function is responsible for formatting PostgreSQL type names with their associated type modifiers. Type modifiers provide additional constraints or specifications for data types (such as precision for numeric types, length for varchar, etc.). The function handles two scenarios:

1. **Default behavior**: When no specific typmod output function is available (), it simply appends the numeric typmod value in parentheses to the type name.

2. **Type-specific formatting**: When a custom typmod output function is provided, it calls that function to get a properly formatted modifier string and appends it to the type name.

The function ensures that typmod values are non-negative (asserts ) and should not be called with the special value -1, which indicates no type modifier.

## Parameters / Member Variables
- : The base name of the PostgreSQL data type (e.g., "varchar", "numeric")
- : The type modifier value (must be >= 0) that specifies constraints like length, precision, etc.
- : OID of the type-specific output function for formatting the typmod, or InvalidOid for default formatting

## Dependencies
- Functions called/Symbols referenced:
  - : Calls the type-specific typmod output function
  - : Converts the function result to a C string
  - : PostgreSQL's sprintf-like function for formatted string creation
  - : Converts int32 to Datum for function call
- Called from (representative examples):
  - : Multiple calls throughout the function for various type formatting scenarios

## Notes and Other Information
- This is a static function within , making it internal to the type formatting module
- The function always returns a newly allocated string that must be freed by the caller
- The assertion  indicates this function should never be called with -1 (the "no typmod" sentinel value)
- Used extensively in PostgreSQL's type system for generating user-friendly representations of typed columns and expressions
- Examples of output: "varchar(50)", "numeric(10,2)", "timestamp(6)"

## Simplified Source

```c
static char *printTypmod(const char *typname, int32 typmod, Oid typmodout) {
    char *result;

    // Assert that typmod is valid (>= 0)
    Assert(typmod >= 0);

    if (typmodout == InvalidOid) {
        // Default: print type name with numeric typmod in parentheses
        result = psprintf("%s(%d)", typname, (int) typmod);
    } else {
        // Use type-specific typmod output function
        char *modifier_str = DatumGetCString(
            OidFunctionCall1(typmodout, Int32GetDatum(typmod))
        );
        result = psprintf("%s%s", typname, modifier_str);
    }

    return result;
}
```
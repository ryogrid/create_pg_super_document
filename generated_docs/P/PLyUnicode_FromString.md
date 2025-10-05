# PLyUnicode_FromString

## Location
[src/pl/plpython/plpy_util.c:118-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_util.c#L118-L121)

## Overview
A convenience wrapper that converts a null-terminated C string in PostgreSQL server encoding to a Python unicode object.

## Definition
```c
PyObject *PLyUnicode_FromString(const char *s)
```

## Detailed Description
PLyUnicode_FromString is a simple wrapper function that provides a convenient interface for converting null-terminated C strings to Python Unicode objects. It internally calls PLyUnicode_FromStringAndSize with the string length calculated using strlen(), making it suitable for typical null-terminated string conversions where the exact length doesn't need to be specified explicitly.

This function is the most commonly used string-to-Unicode conversion function in the PL/Python codebase due to its simplicity and the prevalence of null-terminated strings in C programming.

## Parameters / Member Variables
- `s`: A null-terminated C string in PostgreSQL server encoding to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_FromStringAndSize](PLyUnicode_FromStringAndSize.md)
  - strlen (implicit)
- Called from (representative examples):
  - [set_string_attr](../s/set_string_attr.md)
  - [PLy_trigger_build_args](PLy_trigger_build_args.md) (extensively used - 16 times)
  - [PLy_generate_spi_exceptions](PLy_generate_spi_exceptions.md)
  - [PLy_quote_literal](PLy_quote_literal.md)
  - [PLy_quote_nullable](PLy_quote_nullable.md) (multiple times)
  - [PLy_quote_ident](PLy_quote_ident.md)
  - [PLy_result_colnames](PLy_result_colnames.md)
  - [PLyUnicode_FromScalar](PLyUnicode_FromScalar.md)

## Notes and Other Information
- Returns a new Python Unicode object with transferred reference ownership to the caller
- Most frequently used Unicode conversion function in PL/Python codebase
- Assumes input string is null-terminated - use PLyUnicode_FromStringAndSize for strings with embedded nulls or known length
- Particularly heavily used in trigger argument building (PLy_trigger_build_args) where many string values need conversion
- Inherits all encoding conversion capabilities from PLyUnicode_FromStringAndSize
- Simple one-line implementation makes it an efficient wrapper for the most common use case

## Simplified Source

```c
PyObject *PLyUnicode_FromString(const char *s) {
    // Simply call the size-aware version with calculated length
    return PLyUnicode_FromStringAndSize(s, strlen(s));
}
```
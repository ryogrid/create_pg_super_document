# cstring_to_text

## Location
[src/backend/utils/adt/varlena.c:184-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L184-L195)

## Overview
Creates a PostgreSQL  data type value from a null-terminated C string, providing a bridge between C string data and PostgreSQL's variable-length text type.

## Definition

```c
text *
cstring_to_text(const char *s)
```
## Detailed Description
The  function is a convenience wrapper that converts a null-terminated C string into PostgreSQL's  data type. It internally uses  to determine the length of the input string and delegates the actual conversion to . The resulting  value is freshly allocated using  with a full-size variable header (VARHDR), making it suitable for storage and manipulation within PostgreSQL's memory management system.

This function is part of PostgreSQL's conversion routines exported for use by C code, allowing seamless integration between C string data and PostgreSQL's type system.

## Parameters / Member Variables
- : A null-terminated C string to be converted into a PostgreSQL text value

## Dependencies
- Functions called/Symbols referenced:
  -  - performs the actual conversion with explicit length
  -  - calculates the length of the input C string

- Called from (representative examples):
  -  - text input function
  -  - JSON input processing
  -  - XML input processing
  -  - identifier quoting function
  -  - type formatting function
  -  - size formatting function
  - Various system catalog and utility functions

## Notes and Other Information
- The function allocates memory using PostgreSQL's memory management (), so the returned pointer should be managed within PostgreSQL's memory contexts
- This is a widely used utility function throughout the PostgreSQL codebase for converting C strings to text values
- For cases where the string length is already known,  can be called directly for better performance
- The function is defined in  as part of the variable-length data type utilities
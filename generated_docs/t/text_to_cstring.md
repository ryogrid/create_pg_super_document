# text_to_cstring

## Location
[src/backend/utils/adt/varlena.c:217-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L217-L247)

## Overview
Converts a PostgreSQL text data type value into a null-terminated C string, supporting compressed and toasted text values with automatic detoasting.

## Definition
```c
char *text_to_cstring(const text *t)
```

## Detailed Description
The `text_to_cstring` function creates a palloc'd, null-terminated C string from a PostgreSQL text value. It is designed with robustness in mind, capable of handling compressed or toasted text values by automatically detoasting them using `pg_detoast_datum_packed`. The function extracts the actual data length using `VARSIZE_ANY_EXHDR`, allocates memory for the string plus null terminator, copies the data using `memcpy`, and adds the null terminator.

A notable design consideration is that the function accepts potentially compressed or toasted values, even though such values shouldn't technically be referred to as "text *" in the strict sense. This flexibility provides robustness for various use cases throughout PostgreSQL where text conversion is needed.

## Parameters / Member Variables
- `t`: Pointer to the PostgreSQL text value to be converted (may be compressed or toasted)

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_detoast_datum_packed](../p/pg_detoast_datum_packed.md)` - detoasts compressed or out-of-line values
  - `unconstify` - macro to cast away const qualifier
  - `VARSIZE_ANY_EXHDR` - macro to get data size excluding header
  - `VARDATA_ANY` - macro to get pointer to the actual data
  - `[palloc](../p/palloc.md)` - PostgreSQL's memory allocation function
  - `memcpy` - standard C library function for memory copying
  - `[pfree](../p/pfree.md)` - PostgreSQL's memory deallocation function

- Called from (representative examples):
  - `[pg_notify](../p/pg_notify.md)` - notification functions
  - `[json_object_field_text](../j/json_object_field_text.md)` - JSON field extraction
  - `[quote_ident](../q/quote_ident.md)` - identifier quoting
  - `[parse_ident](../p/parse_ident.md)` - identifier parsing
  - `[to_regproc](to_regproc.md)` - register type conversion functions
  - `[xml_out_internal](../x/xml_out_internal.md)` - XML output processing
  - `[array_to_text](../a/array_to_text.md)` - array to text conversion
  - `[pg_backup_start](../p/pg_backup_start.md)` - backup and recovery functions

## Notes and Other Information
- The function automatically handles memory management for detoasted values, freeing the temporary detoasted copy if it differs from the original input
- The returned C string is allocated using `palloc`, so it should be managed within PostgreSQL's memory context system
- This is a fundamental conversion function used extensively throughout PostgreSQL for interfacing between text data and C string processing
- The function is robust against various text storage formats (compressed, toasted, or regular)
- Essential for functions that need to pass PostgreSQL text data to C library functions expecting null-terminated strings
- Located in `src/backend/utils/adt/varlena.c` as part of the variable-length data type utilities
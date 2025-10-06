# nameconcatoid

## Location
[src/backend/utils/adt/name.c:333-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L333-L355)

## Overview
The nameconcatoid function concatenates a name with an OID to create a unique identifier, primarily used in information_schema views.

## Definition

```c
Datum
nameconcatoid(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements a specialized concatenation operation that combines a PostgreSQL name with an OID (Object Identifier) to create unique identifiers. It's specifically designed for use in information_schema views to generate specific_name columns that must be unique per schema.

The function appends an underscore followed by the OID to the input name. If the resulting string would exceed NAMEDATALEN (PostgreSQL's maximum name length), it intelligently truncates the name portion while preserving the complete OID suffix. This ensures uniqueness is maintained even when truncation occurs.

The function is equivalent to the SQL expression (::text || '_' || ::text)::name but with intelligent length management.

## Parameters / Member Variables
- Parameter 0 (Name): The base name to be concatenated
- Parameter 1 (Oid): The OID to append to the name

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Macro to extract name argument from function call
  - PG_GETARG_OID: Macro to extract OID argument from function call
  - snprintf: Standard C function to format the OID suffix
  - strlen: Standard C function to get the length of the name string
  - [pg_mbcliplen](../p/pg_mbcliplen.md): PostgreSQL function to safely truncate multibyte strings
  - [palloc0](../p/palloc0.md): PostgreSQL memory allocator that zeros the allocated memory
  - memcpy: Standard C function to copy memory regions
  - PG_RETURN_NAME: Macro to return a Name from a PostgreSQL function
  - NAMEDATALEN: PostgreSQL constant defining maximum name length
  - NameStr: Macro to access the string data in a Name

- Called from (representative examples):
  - This function appears to be used primarily in information_schema views rather than being called directly from C code

## Notes and Other Information
- This function is specifically designed for information_schema compliance
- The function ensures uniqueness by appending OIDs, which are globally unique within a database cluster
- Intelligent truncation preserves the uniqueness-ensuring OID suffix while truncating the potentially non-unique name portion
- The function uses palloc0 to ensure the result is properly zero-padded according to PostgreSQL's Name type requirements
- Multibyte character support is provided through pg_mbcliplen to avoid breaking characters during truncation
- The function is defined in src/backend/utils/adt/name.c alongside other name manipulation functions
- Essential for SQL standard compliance in information_schema views

## Simplified Source

```c
Datum
nameconcatoid(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    Oid oid = PG_GETARG_OID(1);
    Name result;
    char suffix[20];
    int suflen, namlen;

    // Create OID suffix "_123456"
    suflen = snprintf(suffix, sizeof(suffix), "_%u", oid);
    namlen = strlen(NameStr(*name));

    // Truncate name if combined length would exceed limit
    if (namlen + suflen >= NAMEDATALEN)
        namlen = pg_mbcliplen(NameStr(*name), namlen, NAMEDATALEN - 1 - suflen);

    // Build result: name + "_" + oid
    result = (Name) palloc0(NAMEDATALEN);
    memcpy(NameStr(*result), NameStr(*name), namlen);
    memcpy(NameStr(*result) + namlen, suffix, suflen);

    PG_RETURN_NAME(result);
}
```
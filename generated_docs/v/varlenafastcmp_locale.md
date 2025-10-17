# varlenafastcmp_locale

## Location
[src/backend/utils/adt/varlena.c:2094-2124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2094-L2124)

## Overview
A sort support comparison function for locale-aware string comparison of variable-length data types (varlena) in PostgreSQL.

## Definition

```c
static int
varlenafastcmp_locale(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
 is a specialized comparison function used in PostgreSQL's sort support framework for locale-aware comparison of variable-length string data types. This function serves as an adapter that extracts string data from PostgreSQL's varlena format and delegates the actual comparison logic to . It handles the conversion from PostgreSQL's Datum representation to raw string pointers and lengths, while ensuring proper memory management to prevent leaks during sorting operations.

The function is designed to work with PostgreSQL's sort support infrastructure, which optimizes sorting operations by providing specialized comparison functions that can be called directly without the overhead of the general-purpose comparison framework.

## Parameters / Member Variables
- `x`: First Datum value to compare (contains a varlena string)
- `y`: Second Datum value to compare (contains a varlena string)
- `ssup`: SortSupport structure containing sorting context and locale information
## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts VarString from Datum with proper detoasting
  -  - Macro to get pointer to actual string data within varlena
  -  - Macro to get length of string data excluding header
  -  - Core locale-aware string comparison function
  -  - Converts pointer back to Datum for memory management check
  -  - PostgreSQL memory deallocation function
- Called from (representative examples):
  -  - Sets up sort support for variable-length string types

## Notes and Other Information
- This function includes careful memory management to prevent leaks when detoasted copies of varlena values are created
- The function checks if the extracted VarString pointers differ from the original Datum pointers and frees temporary copies
- Used specifically for locale-aware sorting where collation rules must be applied
- Part of PostgreSQL's sort support optimization framework for improved sorting performance

## Simplified Source

```c
static int
varlenafastcmp_locale(Datum x, Datum y, SortSupport ssup)
{
    // Extract variable-length strings from Datum values
    VarString *arg1 = DatumGetVarStringPP(x);
    VarString *arg2 = DatumGetVarStringPP(y);

    // Get string data pointers and lengths
    char *string1 = VARDATA_ANY(arg1);
    char *string2 = VARDATA_ANY(arg2);
    int len1 = VARSIZE_ANY_EXHDR(arg1);
    int len2 = VARSIZE_ANY_EXHDR(arg2);

    // Delegate to locale-aware string comparison function
    int result = varstrfastcmp_locale(string1, len1, string2, len2, ssup);

    // Clean up any detoasted copies to prevent memory leaks
    if (PointerGetDatum(arg1) != x)
        pfree(arg1);
    if (PointerGetDatum(arg2) != y)
        pfree(arg2);

    return result;
}
```
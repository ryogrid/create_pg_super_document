# namefastcmp_locale

## Location
[src/backend/utils/adt/varlena.c:2125-2138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2125-L2138)

## Overview
A sort support comparison function for locale-aware comparison of PostgreSQL NAME type data during sorting operations.

## Definition

```c
static int
namefastcmp_locale(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
 is a specialized comparison function designed for locale-aware sorting of PostgreSQL's NAME data type. The NAME type is a fixed-length string type primarily used for system catalog identifiers like table names, column names, and other database object names. This function extracts the string data from NAME-typed Datum values and delegates the actual comparison to , ensuring that locale-specific collation rules are properly applied during sorting operations.

Unlike variable-length strings (varlena), NAME types have a fixed maximum length and are null-terminated, making the extraction process simpler but still requiring length calculation via strlen.

## Parameters / Member Variables
- `x`: First Datum value containing a NAME-typed value to compare
- `y`: Second Datum value containing a NAME-typed value to compare
- `ssup`: SortSupport structure containing sorting context and locale information
## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts Name pointer from Datum
  -  - Macro to get the C string from a Name structure
  -  - Standard C library function to calculate string length
  -  - Core locale-aware string comparison function
- Called from (representative examples):
  -  - Sets up sort support for string types including NAME

## Notes and Other Information
- Specifically optimized for the NAME data type, which is used extensively in PostgreSQL system catalogs
- Unlike varlena comparison functions, this doesn't require complex memory management since NAME types are fixed-size
- The function relies on null-terminated strings and strlen for length calculation
- Part of PostgreSQL's sort support framework for optimized sorting performance
- Used when locale-aware sorting is required for database object names and identifiers

## Simplified Source

```c
static int
namefastcmp_locale(Datum x, Datum y, SortSupport ssup)
{
    // Extract NAME-typed values from Datum
    Name name1 = DatumGetName(x);
    Name name2 = DatumGetName(y);

    // Get null-terminated strings and their lengths, then compare using locale rules
    return varstrfastcmp_locale(NameStr(*name1), strlen(NameStr(*name1)),
                              NameStr(*name2), strlen(NameStr(*name2)),
                              ssup);
}
```
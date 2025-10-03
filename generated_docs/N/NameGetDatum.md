# NameGetDatum

## Location
[src/include/postgres.h:373-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L373-L384)

## Overview
NameGetDatum converts a NameData structure to PostgreSQL's internal Datum representation, enabling Name values to be used in the type system and function interfaces.

## Definition

```c
static inline Datum
NameGetDatum(const NameData *X)
```
## Detailed Description
NameGetDatum converts a NameData structure pointer to a Datum by extracting the string content using the NameStr macro and then calling CStringGetDatum(). This function is essential for converting PostgreSQL's internal Name type (used for identifiers like table names, column names, etc.) into the universal Datum format that can be passed through the function call interface.

The NameData type is a fixed-length structure containing a character array, typically 64 bytes in size (NAMEDATALEN). The NameStr macro extracts the null-terminated string from this structure. This function is the inverse of DatumGetName() and is widely used when storing or manipulating system catalog entries.

## Parameters / Member Variables
- `*X`: A pointer to a NameData structure that will be converted to Datum format
## Dependencies
- Functions called/Symbols referenced:
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - NameStr (macro)
  - [NameData](NameData.md) (type)
- Called from (representative examples):
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md) (attribute catalog management)
  - [TypeCreate](../T/TypeCreate.md) (type system)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md) (constraint management)
  - [OperatorCreate](../O/OperatorCreate.md) (operator system)
  - [RelationBuildTriggers](../R/RelationBuildTriggers.md) (trigger system)
  - PG_RETURN_NAME (function manager macro)

## Notes and Other Information
- Name is pass-by-reference - caller must ensure the NameData structure has adequate lifetime
- The NameData structure contains a fixed-size character array (typically 64 bytes)
- Names in PostgreSQL are limited to NAMEDATALEN-1 characters (usually 63 characters)
- This function is extensively used in system catalog operations
- The conversion extracts the string content without copying the data structure itself
- Used primarily for storing identifiers in system catalogs and processing DDL operations

## Simplified Source

```c
static inline Datum NameGetDatum(const NameData *X)
{
    // Convert NameData structure to Datum by extracting string and
    // converting to C string datum
    return CStringGetDatum(NameStr(*X));
}
```
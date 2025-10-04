# SPI_gettype

## Location
[src/backend/executor/spi.c:1268-1307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1268-L1307)

## Overview
Retrieves the type name of a specified column from a tuple descriptor as a dynamically allocated string.

## Definition
```c
char *SPI_gettype(TupleDesc tupdesc, int fnumber)
```

## Detailed Description
SPI_gettype extracts the PostgreSQL type name for a specified column from a tuple descriptor. It handles both regular user attributes (positive fnumber) and system attributes (negative fnumber). The function performs validation on the attribute number, looks up the type information in the system catalog, and returns a newly allocated string containing the type name.

For regular attributes, it uses TupleDescAttr to access the attribute type OID. For system attributes, it uses SystemAttributeDefinition to get the type information. The function then performs a system cache lookup to retrieve the actual type name from pg_type.

## Parameters / Member Variables
- `tupdesc`: Tuple descriptor containing column information
- `fnumber`: 1-based column number for regular attributes, or negative number for system attributes

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [SystemAttributeDefinition](SystemAttributeDefinition.md)
  - [SearchSysCache1](SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - FirstLowInvalidHeapAttributeNumber
  - SPI_ERROR_NOATTRIBUTE
  - SPI_ERROR_TYPUNKNOWN
  - Form_pg_type
- Called from (representative examples):
  - Limited usage found in header file references

## Notes and Other Information
- Returns NULL and sets SPI_result to SPI_ERROR_NOATTRIBUTE if fnumber is invalid
- Returns NULL and sets SPI_result to SPI_ERROR_TYPUNKNOWN if type lookup fails
- The returned string is dynamically allocated with pstrdup and must be freed by the caller
- Supports both regular attributes (fnumber > 0) and system attributes (fnumber < 0)
- Sets global SPI_result to 0 on success
- Uses system cache for efficient type name lookup with proper cache release

## Simplified Source

```c
char *SPI_gettype(TupleDesc tupdesc, int fnumber) {
    SPI_result = 0;

    // Validate attribute number range
    if (fnumber > tupdesc->natts || fnumber == 0 ||
        fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return NULL;
    }

    // Get type OID from attribute
    Oid type_oid;
    if (fnumber > 0) {
        type_oid = TupleDescAttr(tupdesc, fnumber - 1)->atttypid;  // Regular attribute
    } else {
        type_oid = (SystemAttributeDefinition(fnumber))->atttypid;  // System attribute
    }

    // Look up type name in system catalog
    HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));

    if (!HeapTupleIsValid(type_tuple)) {
        SPI_result = SPI_ERROR_TYPUNKNOWN;
        return NULL;
    }

    // Extract and duplicate type name
    char *type_name = pstrdup(NameStr(((Form_pg_type) GETSTRUCT(type_tuple))->typname));
    ReleaseSysCache(type_tuple);

    return type_name;
}
```
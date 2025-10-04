# SPI_getbinval

## Location
[src/backend/executor/spi.c:1252-1267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1252-L1267)

## Overview
Extracts a column value from a tuple as a Datum, with support for null value detection and attribute number validation.

## Definition

```c
Datum
SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull)
```
## Detailed Description
SPI_getbinval is a fundamental SPI function that extracts the binary value of a specified column from a heap tuple. It performs validation on the attribute number to ensure it's within valid bounds, handles null values appropriately, and delegates the actual value extraction to the lower-level heap_getattr function. This function is essential for accessing tuple data in SPI-based stored procedures and extensions.

The function validates that the field number is within the valid range (1 to natts) and not in the invalid attribute number range. If validation fails, it sets SPI_result to SPI_ERROR_NOATTRIBUTE and returns NULL with the isnull flag set to true.

## Parameters / Member Variables
- `tuple`: The heap tuple from which to extract the column value
- `tupdesc`: Tuple descriptor that describes the structure of the tuple
- `fnumber`: 1-based column number to extract (must be > 0 and <= tupdesc->natts)
- `*isnull`: Pointer to boolean that will be set to true if the value is NULL
## Dependencies
- Functions called/Symbols referenced:
  - [heap_getattr](../h/heap_getattr.md)
  - FirstLowInvalidHeapAttributeNumber
  - SPI_ERROR_NOATTRIBUTE
- Called from (representative examples):
  - [make_ruledef](../m/make_ruledef.md) (src/backend/utils/adt/ruleutils.c)
  - [make_viewdef](../m/make_viewdef.md) (src/backend/utils/adt/ruleutils.c)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (src/backend/utils/adt/tsquery_rewrite.c)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md) (src/backend/utils/adt/tsvector_op.c)
  - [SPI_sql_row_to_xmlelement](SPI_sql_row_to_xmlelement.md) (src/backend/utils/adt/xml.c)

## Notes and Other Information
- Returns (Datum) NULL and sets *isnull = true if the attribute number is invalid
- Sets global SPI_result to SPI_ERROR_NOATTRIBUTE on validation failure, or 0 on success
- The function validates against FirstLowInvalidHeapAttributeNumber to prevent access to invalid system attributes
- Field numbers are 1-based, not 0-based
- This is a higher-level wrapper around heap_getattr that adds SPI-specific error handling and validation

## Simplified Source

```c
Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull) {
    SPI_result = 0;

    // Validate attribute number range
    if (fnumber > tupdesc->natts || fnumber == 0 ||
        fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        *isnull = true;
        return (Datum) NULL;
    }

    // Extract the attribute value using heap function
    return heap_getattr(tuple, fnumber, tupdesc, isnull);
}
```
# SPI_getvalue

## Location
[src/backend/executor/spi.c:1220-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1220-L1251)

## Overview
Extracts a specific attribute value from a tuple and converts it to its string representation using the appropriate output function.

## Definition

```c
char *
SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber)
```
## Detailed Description
SPI_getvalue retrieves the value of a specific attribute from a tuple and converts it to a human-readable string representation. The function handles the complete process of extracting the raw Datum value, checking for NULL values, determining the appropriate data type, and calling the corresponding output function to convert the value to a string. This function is essential for displaying tuple data in a readable format or for converting values for external interfaces.

The function supports both regular attributes (positive attribute numbers) and system attributes (negative attribute numbers). It properly handles NULL values by returning NULL, and uses the PostgreSQL type system to ensure correct string conversion for all data types.

## Parameters / Member Variables
- : The HeapTuple containing the data to extract from
- : The TupleDesc that describes the structure and types of the tuple
- : The 1-based attribute number for regular attributes, or negative number for system attributes

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getattr](../h/heap_getattr.md) (to extract the raw attribute value)
  - TupleDescAttr (macro for accessing tuple descriptor attributes)
  - [SystemAttributeDefinition](SystemAttributeDefinition.md) (for system attribute type information)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (to get the output function for the data type)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md) (to convert the value to string using the appropriate output function)
- Called from (representative examples):
  - [refresh_by_match_merge](../r/refresh_by_match_merge.md) (materialized view operations)
  - [make_ruledef](../m/make_ruledef.md) (rule definition utilities)
  - [make_viewdef](../m/make_viewdef.md) (view definition utilities)

## Notes and Other Information
- Sets SPI_result to SPI_ERROR_NOATTRIBUTE if fnumber is invalid (0, > natts, or <= FirstLowInvalidHeapAttributeNumber)
- Returns NULL if the attribute value is NULL (no string conversion needed)
- Returns a newly allocated string that must be freed by the caller
- Uses the PostgreSQL type system to ensure proper string conversion for all data types
- Handles both built-in types (int4, text, etc.) and user-defined types
- The string representation follows PostgreSQL's standard output format for each data type
- System attributes (ctid, oid, etc.) are properly handled with their specific output functions
- Essential for debugging, logging, and interfacing with external systems that expect string data

## Simplified Source

```c
char *SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber)
{
    Datum val;
    bool isnull;
    Oid typoid, foutoid;
    bool typisvarlena;

    SPI_result = 0;

    // Validate attribute number
    if (fnumber > tupdesc->natts || fnumber == 0 ||
        fnumber <= FirstLowInvalidHeapAttributeNumber)
    {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return NULL;
    }

    // Extract the attribute value from the tuple
    val = heap_getattr(tuple, fnumber, tupdesc, &isnull);
    if (isnull)
        return NULL;

    // Get the data type OID for this attribute
    if (fnumber > 0)
        typoid = TupleDescAttr(tupdesc, fnumber - 1)->atttypid;
    else
        typoid = (SystemAttributeDefinition(fnumber))->atttypid;

    // Get the output function for this data type
    getTypeOutputInfo(typoid, &foutoid, &typisvarlena);

    // Convert the value to string using the appropriate output function
    return OidOutputFunctionCall(foutoid, val);
}
```
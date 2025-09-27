# pg_do_encoding_conversion

## Location
[src/backend/utils/mb/mbutils.c:356-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L356-L468)

## Overview
Converts a string from one encoding to another using PostgreSQL's encoding conversion system, handling memory allocation and validation for the conversion process.

## Definition

```c
unsigned char *
pg_do_encoding_conversion(unsigned char *src, int len,
						  int src_encoding, int dest_encoding)
```
## Detailed Description
This is the core function for performing encoding conversions in PostgreSQL. It takes a source string and converts it from one character encoding to another, handling various edge cases and optimizations:

- Returns the original string if no conversion is needed (same encodings)
- Handles SQL_ASCII encoding specially as it's compatible with any encoding
- Validates that conversions can only happen within a transaction context
- Finds and invokes the appropriate conversion function for the encoding pair
- Allocates memory conservatively to handle worst-case conversion expansion
- Optimizes memory usage by realloc'ing large results to actual size

The function uses PostgreSQL's function call mechanism to invoke encoding-specific conversion procedures and includes robust error handling for unsupported conversions and memory allocation failures.

## Parameters / Member Variables
- : Source string to convert (unsigned char pointer)
- : Length of the source string in bytes
- : Source encoding identifier (integer constant like PG_UTF8)
- : Destination encoding identifier

## Dependencies
- Functions called/Symbols referenced:
  - [pg_verify_mbstr](pg_verify_mbstr.md) (validates multibyte string)
  - [IsTransactionState](../I/IsTransactionState.md) (checks transaction context)
  - [FindDefaultConversionProc](../F/FindDefaultConversionProc.md) (finds conversion function)
  - [pg_encoding_to_char](pg_encoding_to_char.md) (encoding name lookup)
  - [MemoryContextAllocHuge](../M/MemoryContextAllocHuge.md) (memory allocation)
  - OidFunctionCall6 (invokes conversion function)
  - [repalloc](../r/repalloc.md) (memory reallocation)
- Called from (representative examples):
  - [pg_convert](pg_convert.md) (SQL function wrapper)
  - [pg_any_to_server](pg_any_to_server.md) (server encoding conversion)
  - [pg_server_to_any](pg_server_to_any.md) (client encoding conversion)
  - [xml_parse](../x/xml_parse.md) (XML processing)

## Notes and Other Information
- Must be called within a transaction context due to catalog access requirements
- Uses MAX_CONVERSION_GROWTH constant to estimate worst-case memory needs
- Includes overflow protection for very large strings
- For large results (>1MB), optimizes memory usage by shrinking allocated space
- SQL_ASCII encoding is treated specially as universally compatible
- Returns original pointer when no conversion is needed for efficiency

## Simplified Source

```c
// Simplified version of pg_do_encoding_conversion
unsigned char *pg_do_encoding_conversion(unsigned char *src, int len,
                                         int src_encoding, int dest_encoding) {
    // Handle trivial cases first
    if (len <= 0) {
        return src;  // Empty string is always valid
    }

    if (src_encoding == dest_encoding) {
        return src;  // No conversion needed
    }

    if (dest_encoding == PG_SQL_ASCII) {
        return src;  // Any string is valid in SQL_ASCII
    }

    // Special case: converting from SQL_ASCII
    if (src_encoding == PG_SQL_ASCII) {
        // Validate the string in the destination encoding
        pg_verify_mbstr(dest_encoding, (const char *) src, len, false);
        return src;  // No actual conversion possible
    }

    // Ensure we're in a transaction (required for catalog access)
    if (!IsTransactionState()) {
        elog(ERROR, "cannot perform encoding conversion outside a transaction");
    }

    // Find the conversion function for this encoding pair
    Oid proc = FindDefaultConversionProc(src_encoding, dest_encoding);
    if (!OidIsValid(proc)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("default conversion function for encoding \"%s\" to \"%s\" does not exist",
                        pg_encoding_to_char(src_encoding),
                        pg_encoding_to_char(dest_encoding))));
    }

    // Check for integer overflow in memory calculation
    if ((Size) len >= (MaxAllocHugeSize / (Size) MAX_CONVERSION_GROWTH)) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("out of memory"),
                 errdetail("String of %d bytes is too long for encoding conversion.", len)));
    }

    // Allocate buffer for worst-case conversion expansion
    unsigned char *result = (unsigned char *)
        MemoryContextAllocHuge(CurrentMemoryContext,
                               (Size) len * MAX_CONVERSION_GROWTH + 1);

    // Call the conversion function
    OidFunctionCall6(proc,
                     Int32GetDatum(src_encoding),
                     Int32GetDatum(dest_encoding),
                     CStringGetDatum((char *) src),
                     CStringGetDatum((char *) result),
                     Int32GetDatum(len),
                     BoolGetDatum(false));

    // For large results, optimize memory usage by shrinking to actual size
    if (len > 1000000) {
        Size resultlen = strlen((char *) result);

        // Check final result size
        if (resultlen >= MaxAllocSize) {
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("out of memory"),
                     errdetail("String of %d bytes is too long for encoding conversion.", len)));
        }

        // Shrink allocation to actual size
        result = (unsigned char *) repalloc(result, resultlen + 1);
    }

    return result;
}
```

Key simplifications made:
- Added inline comments explaining each major decision point
- Grouped the early-exit conditions at the top for clarity
- Clarified the special handling of SQL_ASCII encoding
- Explained the transaction requirement and catalog access dependency
- Made the memory allocation strategy more explicit
- Simplified the overflow checking logic with clear comments
- Highlighted the conversion function invocation mechanism
- Explained the memory optimization for large results
- Maintained all error handling while improving readability
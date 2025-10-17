# pg_convert_from

## Location
[src/backend/utils/mb/mbutils.c:526-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L526-L552)

## Overview
A SQL function wrapper that converts a bytea string from a specified source encoding to the database encoding, returning the result as text.

## Definition
```c
Datum pg_convert_from(PG_FUNCTION_ARGS)
```

SQL Function Signature:
```sql
TEXT convert_from(BYTEA string, NAME encoding_name)
```

## Detailed Description
This function provides a convenient SQL interface for converting bytea data from any supported encoding to the current database encoding, returning the result as text. It complements pg_convert_to by handling the reverse direction - importing encoded data into the database's native text format.

The function takes a bytea value containing encoded data and the name of the source encoding, then delegates to pg_convert after preparing the destination encoding parameter from the current database encoding. This is particularly useful for importing text data that was encoded in a different character set.

Key aspects:
- Automatically uses the current database encoding as the destination
- Takes bytea input to handle arbitrary byte sequences safely
- Returns text after ensuring the result is valid in the database encoding
- Leverages the structural equivalence of bytea and text varlena types
- Part of PostgreSQL's SQL-accessible encoding conversion interface

## Parameters / Member Variables
- `string`: The bytea containing encoded data to convert (PG_FUNCTION_ARG 0)
- `src_encoding_name`: Name of the source encoding (PG_FUNCTION_ARG 1)

## Dependencies
- Functions called/Symbols referenced:
  - [namein](../n/namein.md) (converts C string to name type)
  - DirectFunctionCall1/DirectFunctionCall3 (function call utilities)
  - [CStringGetDatum](../C/CStringGetDatum.md) (datum conversion)
  - [pg_convert](pg_convert.md) (core conversion function)
  - PG_RETURN_DATUM (result return macro)
- Called from (representative examples):
  - Available as SQL function convert_from()
  - No direct internal references found

## Notes and Other Information
- SQL-accessible function registered in the system catalogs
- Returns bytea from pg_convert as text, relying on varlena type compatibility
- Ensures result validity by converting to the database's known encoding
- Automatically determines destination encoding from current database settings
- Part of PostgreSQL's character set conversion SQL interface alongside convert_to()
- Commonly used for importing text data from external sources with different encodings
- The conversion guarantees that the resulting text is valid in the database encoding

## Simplified Source

```c
Datum
pg_convert_from(PG_FUNCTION_ARGS)
{
    // Extract input bytea and source encoding name
    Datum string = PG_GETARG_DATUM(0);
    Datum src_encoding_name = PG_GETARG_DATUM(1);

    // Get current database encoding as destination encoding
    Datum dest_encoding_name = DirectFunctionCall1(namein,
                                                   CStringGetDatum(DatabaseEncoding->name));

    // Delegate to pg_convert for the actual conversion
    Datum result = DirectFunctionCall3(pg_convert, string,
                                       src_encoding_name, dest_encoding_name);

    return result;
}
```
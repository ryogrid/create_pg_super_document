# pg_convert_to

## Location
src/backend/utils/mb/mbutils.c: 501 - 525

## Overview
A SQL function wrapper that converts a text string from the database encoding to a specified destination encoding, returning the result as bytea.

## Definition
```c
Datum pg_convert_to(PG_FUNCTION_ARGS)
```

SQL Function Signature:
```sql
BYTEA convert_to(TEXT string, NAME encoding_name)
```

## Detailed Description
This function provides a convenient SQL interface for converting text from the current database encoding to any other supported encoding. It acts as a wrapper around the more general pg_convert function by automatically using the current database encoding as the source encoding.

The function takes a text string and an encoding name, then delegates to pg_convert after preparing the source encoding parameter from the current database encoding. This simplifies the common use case where users want to convert from the database's native encoding to another encoding without having to specify the source encoding explicitly.

Key aspects:
- Automatically uses the current database encoding as the source
- Returns bytea rather than text to preserve exact byte sequences
- Leverages the fact that text and bytea have the same varlena structure
- Part of PostgreSQL's SQL-accessible encoding conversion interface

## Parameters / Member Variables
- `string`: The text string to convert (PG_FUNCTION_ARG 0)
- `dest_encoding_name`: Name of the destination encoding (PG_FUNCTION_ARG 1)

## Dependencies
- Functions called/Symbols referenced:
  - namein (converts C string to name type)
  - DirectFunctionCall1/DirectFunctionCall3 (function call utilities)
  - CStringGetDatum (datum conversion)
  - pg_convert (core conversion function)
  - PG_RETURN_DATUM (result return macro)
- Called from (representative examples):
  - Available as SQL function convert_to()
  - No direct internal references found

## Notes and Other Information
- SQL-accessible function registered in the system catalogs
- Relies on structural similarity between text and bytea types for efficient delegation
- Returns bytea to ensure exact preservation of converted byte sequences
- Automatically determines source encoding from current database settings
- Part of PostgreSQL's character set conversion SQL interface alongside convert_from()
- Commonly used in applications that need to export data in specific encodings
# conninfo_uri_parse_options

## Location
src/interfaces/libpq/fe-connect.c: 6375 - 6615

## Overview
Parses a PostgreSQL connection URI string according to RFC 3986 syntax and populates connection options with the extracted values.

## Definition


## Detailed Description
This function is the actual URI parser that handles PostgreSQL connection URIs in the format:


The function supports several advanced features:
- IPv6 addresses enclosed in square brackets
- Multiple netloc:port specifications separated by commas
- Percent-encoding (%xy) in any URI parts
- Comprehensive error handling with detailed error messages

The parsing process involves:
1. Skipping the URI prefix (postgresql://)
2. Extracting user credentials if present (user:password@)
3. Parsing host specifications (including multiple hosts separated by commas)
4. Handling IPv6 addresses in brackets
5. Extracting port numbers
6. Parsing database name
7. Delegating query parameter parsing to conninfo_uri_parse_params

## Parameters / Member Variables
- : Array of PQconninfoOption structures to be populated with parsed values
- : The connection URI string to parse
- : Buffer to store error messages if parsing fails

## Dependencies
- Functions called/Symbols referenced:
  - [uri_prefix_length](../u/uri_prefix_length.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - [conninfo_uri_parse_params](conninfo_uri_parse_params.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - initPQExpBuffer/termPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)/appendPQExpBufferChar
  - PQExpBufferDataBroken
- Called from (representative examples):
  - [conninfo_uri_parse](conninfo_uri_parse.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns true on successful parsing, false on error
- Uses dynamic memory allocation for URI manipulation (strdup)
- Supports PostgreSQL's extension allowing multiple host specifications
- Handles both IPv4 and IPv6 addresses with proper validation
- Avoids setting dbname to empty string to preserve default behavior
- All parsed values are URL-decoded before storage
- Memory cleanup is handled through a goto cleanup pattern
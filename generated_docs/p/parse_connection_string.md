# parse_connection_string

## Location
[src/interfaces/libpq/fe-connect.c:5799-5818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5799-L5818)

## Overview
Central dispatcher function that determines the format of a connection string and routes it to the appropriate parser.

## Definition
```c
static PQconninfoOption *parse_connection_string(const char *connstr,
                                                  PQExpBuffer errorMessage,
                                                  bool use_defaults)
```

## Detailed Description
This function serves as the main entry point for parsing PostgreSQL connection strings in any supported format. It automatically detects whether the input connection string is in URI format (postgresql://...) or traditional keyword=value format, then dispatches to the appropriate specialized parser function.

The function acts as a format-agnostic interface that simplifies connection string handling throughout libpq by providing a single function that can handle both connection string formats transparently. The detection is performed by checking for URI-style prefixes using the uri_prefix_length() function.

## Parameters / Member Variables
- `connstr`: Connection string to parse (can be URI format or keyword=value pairs)
- `errorMessage`: Buffer for storing detailed error messages if parsing fails
- `use_defaults`: Boolean flag indicating whether to apply default values from service files, environment variables, etc.

## Dependencies
- Functions called/Symbols referenced:
  - [uri_prefix_length](../u/uri_prefix_length.md)
  - [conninfo_uri_parse](../c/conninfo_uri_parse.md)
  - [conninfo_parse](../c/conninfo_parse.md)
- Called from (representative examples):
  - internalPQconninfoOption
  - [connectOptions1](../c/connectOptions1.md)
  - [PQconninfoParse](../P/PQconninfoParse.md)
  - [conninfo_array_parse](../c/conninfo_array_parse.md)

## Notes and Other Information
- Returns a malloc'd PQconninfoOption array on success, NULL on failure
- Automatically detects and handles both URI and keyword=value connection string formats
- The use_defaults parameter controls whether default values are populated from external sources
- Essential abstraction layer that simplifies connection string parsing throughout libpq
- All error handling is delegated to the appropriate specialized parser function
- The returned array must be freed by the caller when no longer needed
# conninfo_uri_parse_params

## Location
[src/interfaces/libpq/fe-connect.c:6616-6748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6616-L6748)

## Overview
Parses query parameters from a PostgreSQL connection URI and stores them as connection options with proper URL decoding and validation.

## Definition

```c
static bool
conninfo_uri_parse_params(char *params,
						  PQconninfoOption *connOptions,
						  PQExpBuffer errorMessage)
```
## Detailed Description
This function processes the query parameter portion of a PostgreSQL connection URI (the part after the '?' character). It handles key-value pairs separated by '&' characters and performs the following operations:

1. Parses parameter syntax: key=value&key2=value2...
2. Validates proper key-value separator usage (exactly one '=' per parameter)
3. URL-decodes both keys and values using conninfo_uri_decode
4. Handles special keyword compatibility (converts ssl=true to sslmode=require for JDBC compatibility)
5. Stores valid parameters in the connection options array
6. Provides detailed error reporting for malformed parameters

The function destructively modifies the input params buffer during parsing for efficiency.

## Parameters / Member Variables
- : Query parameter string to parse (will be modified during parsing)
- : Array of PQconninfoOption structures to store parsed parameters
- : Buffer to store error messages if parsing fails

## Dependencies
- Functions called/Symbols referenced:
  - [conninfo_uri_decode](conninfo_uri_decode.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - strcmp
  - free
- Called from (representative examples):
  - [conninfo_uri_parse_options](conninfo_uri_parse_options.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns true on successful parsing, false on error
- Performs memory management for decoded strings (malloc/free)
- Includes JDBC compatibility layer (ssl=true → sslmode=require)
- Ignores unknown parameters rather than failing
- Validates parameter syntax strictly (requires exactly one '=' per parameter)
- Handles both known and unknown connection parameters gracefully
- Uses efficient in-place string modification to minimize memory allocation

## Simplified Source

```c
static bool
conninfo_uri_parse_params(char *params, PQconninfoOption *connOptions, PQExpBuffer errorMessage)
{
    while (*params)
    {
        char *keyword = params;
        char *value = NULL;
        char *p = params;
        bool malloced = false;

        // Parse key=value pairs separated by '&'
        for (;;)
        {
            if (*p == '=')
            {
                if (value != NULL)
                {
                    libpq_append_error(errorMessage, "extra key/value separator \"=\" in URI query parameter: \"%s\"", keyword);
                    return false;
                }
                *p++ = '\0';  // Terminate keyword
                value = p;    // Start of value
            }
            else if (*p == '&' || *p == '\0')
            {
                if (*p != '\0')
                    *p++ = '\0';  // Terminate value, advance to next param

                if (value == NULL)
                {
                    libpq_append_error(errorMessage, "missing key/value separator \"=\" in URI query parameter: \"%s\"", keyword);
                    return false;
                }
                break;  // Found complete key=value pair
            }
            else
                ++p;  // Continue scanning
        }

        // URL decode both keyword and value
        keyword = conninfo_uri_decode(keyword, errorMessage);
        if (keyword == NULL) return false;

        value = conninfo_uri_decode(value, errorMessage);
        if (value == NULL)
        {
            free(keyword);
            return false;
        }
        malloced = true;

        // Handle special JDBC compatibility: ssl=true -> sslmode=require
        if (strcmp(keyword, "ssl") == 0 && strcmp(value, "true") == 0)
        {
            free(keyword);
            free(value);
            keyword = "sslmode";
            value = "require";
            malloced = false;
        }

        // Store the parameter value
        if (!conninfo_storeval(connOptions, keyword, value, errorMessage, true, false))
        {
            if (malloced)
            {
                free(keyword);
                free(value);
            }
            return false;
        }

        // Clean up decoded strings
        if (malloced)
        {
            free(keyword);
            free(value);
        }

        params = p;  // Move to next parameter
    }

    return true;
}
```
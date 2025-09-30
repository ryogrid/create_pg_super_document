# conninfo_parse

## Location
[src/interfaces/libpq/fe-connect.c:5853-6028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5853-L6028)

## Overview
Parses a PostgreSQL connection string containing key=value pairs and returns a structured array of connection options.

## Definition

```c
static PQconninfoOption *
conninfo_parse(const char *conninfo, PQExpBuffer errorMessage,
			   bool use_defaults)
```
## Detailed Description
This function is a core subroutine of  that specifically handles connection strings in key=value pair format. It performs comprehensive parsing of connection parameter strings, handling both quoted and unquoted values, escape sequences, and whitespace normalization.

The parsing process involves several key steps:
1. **Initialization**: Creates a working copy of PQconninfoOptions structure
2. **Tokenization**: Parses the input string to extract parameter name-value pairs
3. **Value Processing**: Handles both quoted (single quotes) and unquoted parameter values
4. **Escape Handling**: Processes backslash escape sequences in values
5. **Storage**: Stores each parsed parameter using 
6. **Default Addition**: Optionally adds default values for unspecified parameters

The function supports two value formats:
- **Unquoted values**: Terminated by whitespace, support backslash escaping
- **Quoted values**: Enclosed in single quotes, support backslash escaping, must be properly terminated

## Parameters / Member Variables
- : The connection string to parse, containing space-separated key=value pairs
- : Buffer for storing detailed error messages if parsing fails
- : Boolean flag indicating whether to add default values for unspecified connection parameters

## Dependencies
- Functions called/Symbols referenced:
  - [conninfo_init](conninfo_init.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - [PQconninfoFree](../P/PQconninfoFree.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - [conninfo_add_defaults](conninfo_add_defaults.md)
  - strdup, free (standard C library functions)
  - isspace (standard C library function)
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:413)
  - [parse_connection_string](../p/parse_connection_string.md) (src/interfaces/libpq/fe-connect.c:5807)

## Notes and Other Information
- This is a static function, meaning it's internal to the fe-connect.c file
- Function performs extensive error checking and provides detailed error messages
- Supports backslash escaping in both quoted and unquoted values
- Memory management: Allocates working buffer that is properly freed on both success and error paths
- Returns NULL on any parsing error, with details stored in errorMessage
- The function modifies a working copy of the input string during parsing
- Quoted strings must be properly terminated or the function will fail with an error
- Whitespace around parameter names, equals signs, and values is properly handled and ignored

## Simplified Source

```c
static PQconninfoOption *conninfo_parse(const char *conninfo, PQExpBuffer errorMessage, bool use_defaults) {
    PQconninfoOption *options;
    char *buf, *cp, *pname, *pval;

    // Initialize connection options structure
    options = conninfo_init(errorMessage);
    if (options == NULL)
        return NULL;

    // Create working copy of input string
    buf = strdup(conninfo);
    if (buf == NULL) {
        libpq_append_error(errorMessage, "out of memory");
        PQconninfoFree(options);
        return NULL;
    }

    cp = buf;
    while (*cp) {
        // Skip whitespace before parameter name
        while (isspace(*cp)) cp++;
        if (!*cp) break;

        // Extract parameter name (until '=' or whitespace)
        pname = cp;
        while (*cp && *cp != '=' && !isspace(*cp)) cp++;

        // Skip whitespace after name
        while (isspace(*cp)) cp++;

        // Expect '=' separator
        if (*cp != '=') {
            libpq_append_error(errorMessage, "missing \"=\" after \"%s\"", pname);
            goto cleanup_error;
        }
        *cp++ = '\0';

        // Skip whitespace after '='
        while (isspace(*cp)) cp++;

        // Extract parameter value (quoted or unquoted)
        pval = cp;
        if (*cp == '\'') {
            // Handle quoted value with escape sequences
            cp = parse_quoted_value(cp, &pval, errorMessage);
            if (cp == NULL) goto cleanup_error;
        } else {
            // Handle unquoted value with escape sequences
            cp = parse_unquoted_value(cp, &pval);
        }

        // Store the parameter
        if (!conninfo_storeval(options, pname, pval, errorMessage, false, false))
            goto cleanup_error;
    }

    free(buf);

    // Add defaults if requested
    if (use_defaults && !conninfo_add_defaults(options, errorMessage)) {
        PQconninfoFree(options);
        return NULL;
    }

    return options;

cleanup_error:
    PQconninfoFree(options);
    free(buf);
    return NULL;
}
```
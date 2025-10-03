# parse_compress_specification

## Location
[src/common/compression.c:107-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/compression.c#L107-L274)

## Overview
A comprehensive parser that processes compression specification strings into structured  objects, supporting algorithm-specific options and parameters.

## Definition

```c
void
parse_compress_specification(pg_compress_algorithm algorithm, char *specification,
							 pg_compress_specification *result)
```
## Detailed Description
The  function parses a compression specification string for a specified algorithm and populates a  result structure. The function handles both simple bare integer compression levels and complex comma-separated keyword=value pairs. It sets appropriate default compression levels based on the algorithm type and validates build-time support for compression libraries. The parser supports compression options like "level", "workers", and "long" (long-distance mode), and provides detailed error reporting through the parse_error field.

## Parameters / Member Variables
- `algorithm`: The  enumeration specifying which compression algorithm to configure
- `*specification`: A null-terminated string containing the compression specification to parse (can be NULL for defaults)
- `*result`: A pointer to a  structure that will be populated with parsed values
## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL string formatting function)
  -  (PostgreSQL string duplication function)
  -  (PostgreSQL memory allocation function)
  -  (PostgreSQL memory deallocation function)
  -  (standard C library function)
  -  (standard C library function)
  -  (standard C library function)
  -  (utility function for parsing integer values)
  -  (utility function for parsing boolean values)
  -  (enumeration type)
  -  (structure type)
  - , , ,  (enumeration constants)
  - ,  (option flag constants)
- Called from (representative examples):
  -  (src/backend/backup/basebackup.c:967)
  -  (src/bin/pg_basebackup/pg_basebackup.c:2658)
  -  (src/bin/pg_receivewal/pg_receivewal.c:805)
  -  (src/bin/pg_dump/pg_dump.c:799)

## Notes and Other Information
- Initializes all fields of the result structure, including setting parse_error to NULL on success
- Sets algorithm-specific default compression levels (0 for LZ4/none, ZSTD_CLEVEL_DEFAULT for ZSTD, Z_DEFAULT_COMPRESSION for gzip)
- Checks compile-time support for compression libraries and reports errors if unavailable
- Supports bare integer specifications (e.g., "6") as shorthand for compression level
- Parses comma-separated keyword=value pairs for advanced options
- Supported keywords: "level" (compression level), "workers" (parallel workers), "long" (long-distance mode)
- Provides detailed error messages for invalid specifications
- Memory management includes proper cleanup of allocated keyword/value strings
- Located in src/common/compression.c for use by both backend and frontend code
- Should be followed by validate_compress_specification to ensure semantic correctness of parsed values

## Simplified Source

```c
// Simplified version of parse_compress_specification
void parse_compress_specification(pg_compress_algorithm algorithm, char *specification,
                                 pg_compress_specification *result) {
    int bare_level;
    char *bare_level_endp;

    // Initialize result structure
    result->algorithm = algorithm;
    result->options = 0;
    result->parse_error = NULL;

    // Set algorithm-specific defaults
    switch (result->algorithm) {
        case PG_COMPRESSION_NONE:
            result->level = 0;
            break;
        case PG_COMPRESSION_LZ4:
            result->level = 0; // fast compression mode
            break;
        case PG_COMPRESSION_ZSTD:
            result->level = ZSTD_CLEVEL_DEFAULT;
            break;
        case PG_COMPRESSION_GZIP:
            result->level = Z_DEFAULT_COMPRESSION;
            break;
    }

    // Handle empty specification
    if (specification == NULL)
        return;

    // Handle bare integer (compression level only)
    bare_level = strtol(specification, &bare_level_endp, 10);
    if (specification != bare_level_endp && *bare_level_endp == '\0') {
        result->level = bare_level;
        return;
    }

    // Parse comma-separated keyword=value pairs
    while (1) {
        char *kwstart, *kwend, *vstart, *vend;
        int kwlen, vlen;
        bool has_value;
        char *keyword, *value;

        // Parse next keyword and optional value
        kwstart = kwend = specification;
        while (*kwend != '\0' && *kwend != ',' && *kwend != '=')
            ++kwend;
        kwlen = kwend - kwstart;

        if (*kwend != '=') {
            vstart = vend = NULL;
            vlen = 0;
            has_value = false;
        } else {
            vstart = vend = kwend + 1;
            while (*vend != '\0' && *vend != ',')
                ++vend;
            vlen = vend - vstart;
            has_value = true;
        }

        // Validate and extract keyword/value
        if (kwlen == 0) {
            result->parse_error = pstrdup(_("found empty string where a compression option was expected"));
            break;
        }

        keyword = palloc(kwlen + 1);
        memcpy(keyword, kwstart, kwlen);
        keyword[kwlen] = '\0';

        if (has_value) {
            value = palloc(vlen + 1);
            memcpy(value, vstart, vlen);
            value[vlen] = '\0';
        } else {
            value = NULL;
        }

        // Process recognized keywords
        if (strcmp(keyword, "level") == 0) {
            result->level = expect_integer_value(keyword, value, result);
        } else if (strcmp(keyword, "workers") == 0) {
            result->workers = expect_integer_value(keyword, value, result);
            result->options |= PG_COMPRESSION_OPTION_WORKERS;
        } else if (strcmp(keyword, "long") == 0) {
            result->long_distance = expect_boolean_value(keyword, value, result);
            result->options |= PG_COMPRESSION_OPTION_LONG_DISTANCE;
        } else {
            result->parse_error = psprintf(_("unrecognized compression option: \"%s\""), keyword);
        }

        // Cleanup and check for completion
        pfree(keyword);
        if (value != NULL)
            pfree(value);

        if (result->parse_error != NULL ||
            (vend == NULL ? *kwend == '\0' : *vend == '\0'))
            break;

        // Move to next option
        specification = vend == NULL ? kwend + 1 : vend + 1;
    }
}
```

Key simplifications made:
- Removed conditional compilation guards for readability
- Simplified algorithm default setting logic
- Consolidated keyword/value parsing logic
- Maintained comprehensive option parsing functionality
- Preserved error handling and memory management
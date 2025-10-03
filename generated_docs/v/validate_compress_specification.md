# validate_compress_specification

## Location
[src/common/compression.c:344-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/compression.c#L344-L425)

## Overview
Validates a parsed compression specification to ensure it is semantically correct and uses appropriate parameters for the specified compression algorithm.

## Definition

```c
char *
validate_compress_specification(pg_compress_specification *spec)
```
## Detailed Description
This function validates a previously parsed compression specification structure to ensure semantic correctness. It performs comprehensive validation of compression parameters including:

- Verifies that compression levels are within the valid range for each algorithm
- Ensures algorithm-specific options are only used with compatible compression methods  
- Validates that compression algorithms support the requested features (workers, long-distance mode)

The function supports validation for GZIP, LZ4, ZSTD, and NONE compression algorithms. It does not test whether the current PostgreSQL build actually supports the requested compression method - it only validates the logical correctness of the specification.

For compression levels, the function enforces algorithm-specific ranges:
- GZIP: 1-9 (with Z_DEFAULT_COMPRESSION as default if libz available)
- LZ4: 1-12 (with 0 as fast mode default)  
- ZSTD: ZSTD_minCLevel() to ZSTD_maxCLevel() (with ZSTD_CLEVEL_DEFAULT as default)
- NONE: Must be 0 (no compression level allowed)

Advanced features validation:
- Worker threads: Only supported by ZSTD algorithm
- Long-distance mode: Only supported by ZSTD algorithm

## Parameters / Member Variables
- `*spec`: Pointer to pg_compress_specification structure containing the parsed compression specification to validate
## Dependencies
- Functions called/Symbols referenced:
  - [get_compress_algorithm_name](../g/get_compress_algorithm_name.md)
  - [psprintf](../p/psprintf.md)
  - PG_COMPRESSION_GZIP
  - PG_COMPRESSION_LZ4
  - PG_COMPRESSION_ZSTD
  - PG_COMPRESSION_NONE
  - PG_COMPRESSION_OPTION_WORKERS
  - PG_COMPRESSION_OPTION_LONG_DISTANCE
- Called from (representative examples):
  - [parse_basebackup_options](../p/parse_basebackup_options.md) (src/backend/backup/basebackup.c:970)
  - [main](../m/main.md) (src/bin/pg_basebackup/pg_basebackup.c:2659)
  - [main](../m/main.md) (src/bin/pg_basebackup/pg_receivewal.c:807)
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:801)

## Notes and Other Information
- Returns NULL if validation succeeds, or an error message string if validation fails
- Must be called after successfully parsing a compression specification string
- The function assumes the spec structure has already been populated by a parsing function
- Does not verify build-time availability of compression libraries - only validates logical parameter correctness
- Supports internationalization through gettext (_("...")) for error messages

## Simplified Source

```c
// Simplified version of validate_compress_specification
char *validate_compress_specification(pg_compress_specification *spec) {
    int min_level = 1;
    int max_level = 1;
    int default_level = 0;

    // Check for parse errors first
    if (spec->parse_error != NULL)
        return spec->parse_error;

    // Set algorithm-specific level ranges
    switch (spec->algorithm) {
        case PG_COMPRESSION_GZIP:
            max_level = 9;
            default_level = Z_DEFAULT_COMPRESSION;
            break;
        case PG_COMPRESSION_LZ4:
            max_level = 12;
            default_level = 0; // fast mode
            break;
        case PG_COMPRESSION_ZSTD:
            max_level = ZSTD_maxCLevel();
            min_level = ZSTD_minCLevel();
            default_level = ZSTD_CLEVEL_DEFAULT;
            break;
        case PG_COMPRESSION_NONE:
            if (spec->level != 0)
                return psprintf(_("compression algorithm \"%s\" does not accept a compression level"),
                                get_compress_algorithm_name(spec->algorithm));
            break;
    }

    // Validate compression level
    if ((spec->level < min_level || spec->level > max_level) &&
        spec->level != default_level)
        return psprintf(_("compression algorithm \"%s\" expects a compression level between %d and %d (default at %d)"),
                        get_compress_algorithm_name(spec->algorithm),
                        min_level, max_level, default_level);

    // Validate algorithm-specific options
    if ((spec->options & PG_COMPRESSION_OPTION_WORKERS) != 0 &&
        (spec->algorithm != PG_COMPRESSION_ZSTD))
        return psprintf(_("compression algorithm \"%s\" does not accept a worker count"),
                        get_compress_algorithm_name(spec->algorithm));

    if ((spec->options & PG_COMPRESSION_OPTION_LONG_DISTANCE) != 0 &&
        (spec->algorithm != PG_COMPRESSION_ZSTD))
        return psprintf(_("compression algorithm \"%s\" does not support long-distance mode"),
                        get_compress_algorithm_name(spec->algorithm));

    return NULL; // Success
}
```

Key simplifications made:
- Removed conditional compilation guards for clarity
- Streamlined switch case structure
- Consolidated validation logic
- Maintained all essential validation checks
- Preserved error message formatting
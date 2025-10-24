# json_manifest_finalize_file

## Location
[src/common/parse_manifest.c:649-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L649-L750)

## Overview
Performs comprehensive parsing, validation, and processing of file information from JSON manifest data, including pathname decoding, size parsing, and checksum validation before invoking a per-file callback.

## Definition
```c
static void
json_manifest_finalize_file(JsonManifestParseState *parse)
```

## Detailed Description
This function is the core file processing routine for JSON manifest parsing in PostgreSQL's backup system. It handles all aspects of individual file entry processing:

1. **Field Validation**: Ensures required fields (pathname/encoded_pathname and size) are present and mutually exclusive where appropriate
2. **Pathname Processing**: Decodes hex-encoded pathnames when necessary and validates the decoding process
3. **Size Parsing**: Converts string size representation to numeric value with error checking
4. **Checksum Processing**: Parses checksum algorithm and payload, supporting various checksum types
5. **Memory Management**: Properly frees allocated memory after processing
6. **Callback Invocation**: Calls the per-file callback with all processed file information

The function is designed to handle both regular pathnames and hex-encoded pathnames (for filenames containing special characters), and supports various checksum algorithms for data integrity verification.

## Parameters / Member Variables
- `parse`: Pointer to JsonManifestParseState structure containing parsed file information including pathname, encoded_pathname, size, algorithm, and checksum fields

## Dependencies
- Functions called/Symbols referenced:
  - `[json_manifest_parse_failure](json_manifest_parse_failure.md)` - [error](../e/error.md) reporting for parsing failures
  - `[palloc](../p/palloc.md)` - PostgreSQL memory allocation
  - `[pfree](../p/pfree.md)` - PostgreSQL memory deallocation  
  - `[hexdecode_string](../h/hexdecode_string.md)` - hex string to binary conversion
  - `strtoul` - string to unsigned long conversion
  - `[pg_checksum_parse_type](../p/pg_checksum_parse_type.md)` - checksum algorithm type parsing
  - `strlen` - string length calculation
  - `CHECKSUM_TYPE_NONE` - constant for no checksum type
  - `JsonManifestParseState` - parsing state structure
  - `[JsonManifestParseContext](../J/JsonManifestParseContext.md)` - parsing context structure
  - `pg_checksum_type` - checksum type enumeration
- Called from (representative examples):
  - `[json_manifest_object_end](json_manifest_object_end.md)` - JSON object completion handler
  - Used in `JsonManifestParseIncrementalState` structure

## Notes and Other Information
- This is a static function, only accessible within the parse_manifest.c file
- Handles both regular pathnames and hex-encoded pathnames for special character support
- Implements comprehensive error checking for all file attributes
- Supports multiple checksum algorithms through the `pg_checksum_parse_type` function
- Performs proper memory cleanup after processing to prevent memory leaks
- The function enforces that pathname and encoded_pathname are mutually exclusive
- Checksum validation includes both algorithm validation and payload hex-decoding
- Part of PostgreSQL's backup manifest infrastructure for ensuring backup integrity
- Uses PostgreSQL's internal memory management functions (palloc/pfree) for consistent memory handling

## Simplified Source

```c
static void json_manifest_finalize_file(JsonManifestParseState *parse) {
    JsonManifestParseContext *context = parse->context;
    size_t size;
    char *ep;
    int checksum_string_length;
    pg_checksum_type checksum_type;
    int checksum_length;
    uint8 *checksum_payload;

    // Validate required fields
    if (parse->pathname == NULL && parse->encoded_pathname == NULL)
        json_manifest_parse_failure(parse->context, "missing path name");
    if (parse->pathname != NULL && parse->encoded_pathname != NULL)
        json_manifest_parse_failure(parse->context, "both path name and encoded path name");
    if (parse->size == NULL)
        json_manifest_parse_failure(parse->context, "missing size");
    if (parse->algorithm == NULL && parse->checksum != NULL)
        json_manifest_parse_failure(parse->context, "checksum without algorithm");

    // Decode hex-encoded pathname if needed
    if (parse->encoded_pathname != NULL) {
        int encoded_length = strlen(parse->encoded_pathname);
        int raw_length = encoded_length / 2;

        parse->pathname = palloc(raw_length + 1);
        if (encoded_length % 2 != 0 ||
            !hexdecode_string((uint8 *) parse->pathname,
                             parse->encoded_pathname, raw_length))
            json_manifest_parse_failure(parse->context, "could not decode file name");

        parse->pathname[raw_length] = '\0';
        pfree(parse->encoded_pathname);
        parse->encoded_pathname = NULL;
    }

    // Parse file size
    size = strtoul(parse->size, &ep, 10);
    if (*ep)
        json_manifest_parse_failure(parse->context, "file size is not an integer");

    // Parse checksum algorithm
    if (parse->algorithm == NULL)
        checksum_type = CHECKSUM_TYPE_NONE;
    else if (!pg_checksum_parse_type(parse->algorithm, &checksum_type))
        context->error_cb(context, "unrecognized checksum algorithm: \"%s\"",
                         parse->algorithm);

    // Parse checksum payload
    checksum_string_length = parse->checksum == NULL ? 0 : strlen(parse->checksum);
    if (checksum_string_length == 0) {
        checksum_length = 0;
        checksum_payload = NULL;
    } else {
        checksum_length = checksum_string_length / 2;
        checksum_payload = palloc(checksum_length);
        if (checksum_string_length % 2 != 0 ||
            !hexdecode_string(checksum_payload, parse->checksum, checksum_length))
            context->error_cb(context,
                             "invalid checksum for file \"%s\": \"%s\"",
                             parse->pathname, parse->checksum);
    }

    // Invoke callback with processed file information
    context->per_file_cb(context, parse->pathname, size,
                        checksum_type, checksum_length, checksum_payload);

    // Clean up allocated memory
    if (parse->size != NULL) {
        pfree(parse->size);
        parse->size = NULL;
    }
    if (parse->algorithm != NULL) {
        pfree(parse->algorithm);
        parse->algorithm = NULL;
    }
    if (parse->checksum != NULL) {
        pfree(parse->checksum);
        parse->checksum = NULL;
    }
}
```
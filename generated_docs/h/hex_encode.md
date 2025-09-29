# hex_encode

## Location
[src/backend/utils/adt/encode.c:162-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L162-L175)

## Overview
Internal utility function that converts binary data to hexadecimal string representation, used as a building block for PostgreSQL's hex encoding functionality.

## Definition

```c
uint64
hex_encode(const char *src, size_t len, char *dst)
```
## Detailed Description
The `hex_encode` function performs low-level hexadecimal encoding of binary data. It converts each input byte into two hexadecimal characters using a lookup table. Each byte is split into its high and low 4-bit nibbles, which are then converted to their corresponding hexadecimal characters using the `hextbl` lookup table ("0123456789abcdef").

This is a core utility function that provides efficient hex encoding without memory allocation, requiring the caller to provide a pre-allocated destination buffer of sufficient size (at least 2 * input length).

## Parameters / Member Variables
- `src`: Pointer to source binary data to be encoded
- `len`: Length of the source data in bytes
- `dst`: Pointer to destination buffer for the hexadecimal string (must be pre-allocated with at least 2 * len bytes)

## Dependencies
- Functions called/Symbols referenced:
  - `hextbl` - Static lookup table containing "0123456789abcdef"
- Called from (representative examples):
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md) - Backup manifest file handling
  - [SendBackupManifest](../S/SendBackupManifest.md) - Backup manifest transmission
  - [byteaout](../b/byteaout.md) - Binary data output formatting
  - `[esc_dec_len](../e/esc_dec_len.md)` - Escape encoding/decoding operations
  - [manifest_writer](../m/manifest_writer.md) - Backup manifest writing utilities
  - `[add_file_to_manifest](../a/add_file_to_manifest.md)` - Adding files to backup manifests
  - `[finalize_manifest](../f/finalize_manifest.md)` - Finalizing backup manifests

## Notes and Other Information
- Returns the exact number of output characters (always 2 * input length)
- No memory allocation - caller must provide adequately sized destination buffer
- Uses bitwise operations for efficient nibble extraction: high nibble via right shift by 4 bits, low nibble via bitwise AND with 0xF
- Part of PostgreSQL's core encoding utilities, heavily used in backup and manifest operations
- Produces lowercase hexadecimal output (uses 'abcdef' not 'ABCDEF')
- No null termination of output string - caller responsible if needed

## Simplified Source

```c
uint64
hex_encode(const char *src, size_t len, char *dst)
{
    const char *end = src + len;

    while (src < end)
    {
        *dst++ = hextbl[(*src >> 4) & 0xF];
        *dst++ = hextbl[*src & 0xF];
        src++;
    }
    return (uint64) len * 2;
}
```
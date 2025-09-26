# hexdecode_string

## Location
[src/common/parse_manifest.c:918-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L918-L938)

## Overview
A static function that converts a hexadecimal string representation into a byte array, processing two hexadecimal characters per output byte.

## Definition
```c
static bool hexdecode_string(uint8 *result, char *input, int nbytes)
```

## Detailed Description
The `hexdecode_string` function performs bulk conversion of hexadecimal string data to binary byte arrays. It processes the input string in pairs of hexadecimal characters, where each pair represents one byte in the output. The function is designed for parsing hexadecimal-encoded data commonly found in backup manifests, such as checksums, file hashes, and other binary data stored in text format.

The conversion process reads two consecutive characters from the input string, converts each to its hexadecimal value using `hexdecode_char`, and combines them to form a single byte value. The high-order nibble comes from the first character, and the low-order nibble comes from the second character. This follows standard hexadecimal string conventions.

The function provides error handling by returning false if any invalid hexadecimal characters are encountered during processing, allowing the caller to handle malformed input appropriately.

## Parameters / Member Variables
- `result`: Output buffer to store the decoded byte array (must be pre-allocated with sufficient space)
- `input`: Input hexadecimal string to be decoded (should contain exactly 2*nbytes characters)
- `nbytes`: Number of bytes to decode (determines how many character pairs to process)

## Return Value
- Returns `true` if all characters were successfully decoded
- Returns `false` if invalid hexadecimal characters are encountered

## Dependencies
- Functions called/Symbols referenced:
  - [hexdecode_char](hexdecode_char.md) (called twice per byte to decode character pairs)

- Called from (representative examples):
  - [json_manifest_finalize_file](../j/json_manifest_finalize_file.md) (for file checksum decoding)
  - [verify_manifest_checksum](../v/verify_manifest_checksum.md) (for manifest checksum verification)
  - [JsonManifestParseIncrementalState](../J/JsonManifestParseIncrementalState.md) (structure reference)

## Notes and Other Information
- This is a static function, limiting its scope to the parse_manifest.c compilation unit
- Assumes input string contains exactly 2*nbytes hexadecimal characters
- Does not perform bounds checking on the input string length
- Each output byte is formed by combining two hex characters: result[i] = n1 * 16 + n2
- Used primarily for decoding checksums and hash values stored in backup manifests
- The caller is responsible for allocating sufficient space in the result buffer
- Processing stops immediately upon encountering the first invalid character
# pg_wchar2euc_with_len

## Location
[src/common/wchar.c:377-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L377-L422)

## Overview
Converts PostgreSQL wide character representation back to EUC (Extended Unix Code) multibyte encoding format with specified length limit.

## Definition
```c
static int pg_wchar2euc_with_len(const pg_wchar *from, unsigned char *to, int len)
```

## Detailed Description
This function performs the reverse conversion of EUC-to-wchar conversion, transforming PostgreSQL's internal wide character format (pg_wchar) back into EUC multibyte byte sequences. The function is generic for all EUC variants (EUC-JP, EUC-KR, EUC-TW, etc.) and reconstructs the original multibyte sequences by extracting bytes from different bit positions within each pg_wchar value:

- **4-byte sequences**: Extracts from all 4 byte positions (bits 31-24, 23-16, 15-8, 7-0)
- **3-byte sequences**: Extracts from 3 byte positions (bits 23-16, 15-8, 7-0)  
- **2-byte sequences**: Extracts from 2 byte positions (bits 15-8, 7-0)
- **1-byte sequences**: Extracts from lowest byte position (bits 7-0)

The bit pattern analysis allows the function to reconstruct the exact original EUC byte sequence that was encoded by the corresponding wchar-to-EUC conversion function.

## Parameters / Member Variables
- `from`: Pointer to the input array of pg_wchar values to convert
- `to`: Pointer to the output buffer where converted EUC byte sequence will be stored
- `len`: Maximum number of pg_wchar values to process from the source array

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic bit manipulation operations)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through encoding conversion tables for multiple EUC variants)

## Notes and Other Information
- Returns the number of bytes written to the output buffer (not including null terminator)
- Always null-terminates the output byte sequence
- Caller must ensure adequate space allocation in the output buffer
- Generic function used for all EUC encoding variants (JP, KR, TW, etc.)
- Reconstructs variable-length multibyte sequences (1-4 bytes per character)
- The bit extraction pattern corresponds to how the original EUC-to-wchar functions encoded the sequences
- Essential component of PostgreSQL's bidirectional character encoding conversion system

## Simplified Source

```c
static int pg_wchar2euc_with_len(const pg_wchar *from, unsigned char *to, int len) {
    int cnt = 0;

    // Convert each wide character back to EUC multibyte format
    while (len > 0 && *from) {
        unsigned char c;

        // Check for 4-byte sequence (bits 31-24 set)
        if ((c = (*from >> 24))) {
            *to++ = c;
            *to++ = (*from >> 16) & 0xff;
            *to++ = (*from >> 8) & 0xff;
            *to++ = *from & 0xff;
            cnt += 4;
        }
        // Check for 3-byte sequence (bits 23-16 set)
        else if ((c = (*from >> 16))) {
            *to++ = c;
            *to++ = (*from >> 8) & 0xff;
            *to++ = *from & 0xff;
            cnt += 3;
        }
        // Check for 2-byte sequence (bits 15-8 set)
        else if ((c = (*from >> 8))) {
            *to++ = c;
            *to++ = *from & 0xff;
            cnt += 2;
        }
        // Single-byte ASCII character
        else {
            *to++ = *from;
            cnt++;
        }
        from++;
        len--;
    }

    *to = 0;  // Null terminate
    return cnt;  // Return number of bytes written
}
```
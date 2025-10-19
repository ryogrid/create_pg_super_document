# inet_cidr_ntop_ipv6

## Location
[src/backend/utils/adt/inet_cidr_ntop.c:165-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_cidr_ntop.c#L165-L294)

## Overview
Converts IPv6 network addresses from binary format to CIDR presentation format with zero compression optimization and IPv4-mapped address detection.

## Definition
```c
static char *inet_cidr_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)
```

## Detailed Description
This static function handles the complex conversion of IPv6 network addresses from their 16-byte binary representation to human-readable CIDR format. It implements sophisticated formatting logic including zero compression (finding and collapsing the longest sequence of zero fields), IPv4-mapped address detection, and intelligent prefix-based truncation.

The function operates in several phases: input validation, network masking based on prefix length, zero sequence detection and compression, special IPv4-mapped address handling, and final CIDR notation formatting. It uses an internal buffer to build the output safely before copying to the destination.

Key algorithmic features include finding the longest consecutive sequence of zero 16-bit words for compression (:: notation), detecting IPv4-mapped IPv6 addresses for mixed notation output, and handling partial word formatting when the prefix length doesn't align with 16-bit boundaries.

## Parameters / Member Variables
- `src`: Pointer to the source IPv6 address in network byte order (16 bytes)  
- `bits`: Number of network prefix bits (0-128), determines formatting scope
- `dst`: Destination buffer to store the formatted CIDR string
- `size`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - SPRINTF (macro for safe string formatting)
  - memcpy (memory copy for address buffer)
  - memset (memory initialization for zero padding)
  - strlen (string length calculation)
  - strcpy (final string copy to destination)
  - EINVAL (error code for invalid bit count)
  - EMSGSIZE (error code for insufficient buffer space)
- Called from (representative examples):
  - [pg_inet_cidr_ntop](../p/pg_inet_cidr_ntop.md) (when processing IPv6 addresses)

## Notes and Other Information
- Returns pointer to destination buffer on success, NULL on error with errno set
- Input validation ensures bits parameter is within valid range (0-128)  
- Uses internal 16-byte buffer for network masking and output formatting buffer
- Special case handling for zero-length networks (bits=0) outputs "::"
- Zero compression algorithm finds longest consecutive zero sequence for :: notation
- IPv4-mapped address detection for addresses like ::ffff:192.0.2.1
- [Complex](../C/Complex.md) logic handles various IPv6 formatting edge cases and optimizations
- Network byte order assumed for all input processing
- Originally based on Paul Vixie's IPv4 version, adapted by Vadim Kogan (UCB) in June 2001
- Function is static (internal linkage) within the inet_cidr_ntop.c module
- Output buffer size requirement can be up to 49 characters for full IPv6 CIDR notation

## Simplified Source

```c
static char *inet_cidr_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size) {
    char outbuf[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
    unsigned char inbuf[16];
    char *cp = outbuf;

    // Validate prefix length
    if (bits < 0 || bits > 128) {
        errno = EINVAL;
        return NULL;
    }

    // Special case for zero-length network
    if (bits == 0) {
        strcpy(outbuf, "::");
    } else {
        // Copy and mask the network portion
        int bytes = (bits + 7) / 8;
        memcpy(inbuf, src, bytes);
        memset(inbuf + bytes, 0, 16 - bytes);

        // Mask partial byte if needed
        int remaining_bits = bits % 8;
        if (remaining_bits != 0) {
            u_int mask = ((u_int) ~0) << (8 - remaining_bits);
            inbuf[bytes - 1] &= mask;
        }

        // Calculate words to display
        int words = (bits + 15) / 16;
        if (words == 1) words = 2;

        // Find longest zero sequence for compression
        int zero_start = 0, zero_len = 0;
        int tmp_start = 0, tmp_len = 0;

        for (int i = 0; i < words * 2; i += 2) {
            if ((inbuf[i] | inbuf[i + 1]) == 0) {
                if (tmp_len == 0) tmp_start = i / 2;
                tmp_len++;
            } else {
                if (tmp_len > zero_len) {
                    zero_start = tmp_start;
                    zero_len = tmp_len;
                }
                tmp_len = 0;
            }
        }
        if (tmp_len > zero_len) {
            zero_start = tmp_start;
            zero_len = tmp_len;
        }

        // Check for IPv4-mapped addresses
        bool is_ipv4 = (zero_len != words && zero_start == 0 &&
                       (zero_len == 6 || (zero_len == 5 &&
                        inbuf[10] == 0xff && inbuf[11] == 0xff)));

        // Format the address
        cp = outbuf;
        for (int p = 0; p < words; p++) {
            // Handle zero compression
            if (zero_len > 0 && p >= zero_start && p < zero_start + zero_len) {
                if (p == zero_start) *cp++ = ':';
                if (p == words - 1) *cp++ = ':';
                continue;
            }

            // Handle IPv4-mapped notation
            if (is_ipv4 && p > 5) {
                *cp++ = (p == 6) ? ':' : '.';
                cp += sprintf(cp, "%u", inbuf[p * 2]);
                if (p != 7 || bits > 120) {
                    *cp++ = '.';
                    cp += sprintf(cp, "%u", inbuf[p * 2 + 1]);
                }
            } else {
                // Regular IPv6 hexadecimal notation
                if (cp != outbuf) *cp++ = ':';
                int word_val = inbuf[p * 2] * 256 + inbuf[p * 2 + 1];
                cp += sprintf(cp, "%x", word_val);
            }
        }
    }

    // Add CIDR prefix
    sprintf(cp, "/%u", bits);

    // Check buffer size and copy result
    if (strlen(outbuf) + 1 > size) {
        errno = EMSGSIZE;
        return NULL;
    }
    strcpy(dst, outbuf);
    return dst;
}
```
# inet_cidr_pton_ipv4

## Location
[src/backend/utils/adt/inet_net_pton.c:97-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L97-L259)

## Overview
Converts IPv4 network numbers from presentation format to network format, supporting hexadecimal, decimal octets, and CIDR notation with automatic classful network inference.

## Definition

```c
struction unless we prefetched EOS. */
	if (ch != '\0')
		goto enoent;
```
## Detailed Description
This function parses IPv4 network addresses from string format into binary network format. It supports multiple input formats including hexadecimal notation (0x prefix), decimal dotted notation (192.168.1.0), and CIDR specifications (/24 suffix). When no CIDR specification is provided, the function automatically infers the network width based on classful networking rules (Class A, B, C, D, E).

The function handles hexadecimal input by consuming nybble strings and decimal input by parsing dotted decimal notation. It validates that decimal octets don't exceed 255 and ensures proper format compliance. For CIDR specifications, it parses the /prefix notation and validates that the prefix length doesn't exceed 32 bits.

The network byte order is assumed throughout the conversion process, meaning that network addresses like 192.5.5.240/28 will have the binary pattern 0b11110000 in the fourth octet.

## Parameters / Member Variables
- : Source string containing the IPv4 network address in presentation format
- : Destination buffer to store the converted binary network address
- : Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - EMSGSIZE (error constant for message size errors)
  - Standard C library functions: isxdigit, isupper, tolower, strchr, isdigit
- Called from (representative examples):
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md) (src/backend/utils/adt/inet_net_pton.c:69)

## Notes and Other Information
- Returns the number of bits in the network specification or -1 on failure
- Supports automatic classful network inference when no CIDR is specified:
  - Class A (0-127): 8 bits default
  - Class B (128-191): 16 bits default  
  - Class C (192-223): 24 bits default
  - Class D (224-239): 8 bits default (4 bits for 224.0.0.0 exactly)
  - Class E (240-255): 32 bits default
- Error handling sets errno to ENOENT for invalid specifications or EMSGSIZE for buffer overflow
- Hexadecimal format supports both uppercase and lowercase digits
- The function extends the network representation to cover the actual mask if needed
- Validates that CIDR prefix length doesn't exceed 32 bits for IPv4

## Simplified Source

```c
static int inet_cidr_pton_ipv4(const char *src, u_char *dst, size_t size) {
    const u_char *odst = dst;
    int ch = *src++;
    int bits = -1;

    // Buffer size validation
    if (size <= 0U) goto emsgsize;

    // Parse hexadecimal format (0x prefix)
    if (ch == '0' && (src[0] == 'x' || src[0] == 'X') &&
        isxdigit((unsigned char) src[1])) {

        src++; // Skip 'x' or 'X'
        int tmp = 0, dirty = 0;

        while ((ch = *src++) != '\0' && isxdigit((unsigned char) ch)) {
            int n = (ch >= '0' && ch <= '9') ? ch - '0' :
                    (tolower(ch) >= 'a' && tolower(ch) <= 'f') ? tolower(ch) - 'a' + 10 : 0;

            if (dirty == 0) {
                tmp = n;
            } else {
                tmp = (tmp << 4) | n;
            }

            if (++dirty == 2) {
                if (size-- <= 0U) goto emsgsize;
                *dst++ = (u_char) tmp;
                dirty = 0;
            }
        }

        // Handle odd trailing nybble
        if (dirty) {
            if (size-- <= 0U) goto emsgsize;
            *dst++ = (u_char) (tmp << 4);
        }
    }
    // Parse decimal dotted notation
    else if (isdigit((unsigned char) ch)) {
        for (;;) {
            int tmp = 0;

            // Parse one decimal octet
            do {
                int n = ch - '0';
                tmp = tmp * 10 + n;
                if (tmp > 255) goto enoent;
            } while ((ch = *src++) != '\0' && isdigit((unsigned char) ch));

            if (size-- <= 0U) goto emsgsize;
            *dst++ = (u_char) tmp;

            if (ch == '\0' || ch == '/') break;
            if (ch != '.') goto enoent;

            ch = *src++;
            if (!isdigit((unsigned char) ch)) goto enoent;
        }
    }
    else {
        goto enoent; // Invalid format
    }

    // Parse CIDR prefix if present
    if (ch == '/' && isdigit((unsigned char) src[0]) && dst > odst) {
        ch = *src++; // Skip '/'
        bits = 0;

        do {
            int n = ch - '0';
            bits = bits * 10 + n;
        } while ((ch = *src++) != '\0' && isdigit((unsigned char) ch));

        if (ch != '\0') goto enoent;
        if (bits > 32) goto emsgsize;
    }

    // Must reach end of string
    if (ch != '\0') goto enoent;
    if (dst == odst) goto enoent; // No address found

    // Infer classful network width if no CIDR specified
    if (bits == -1) {
        if (*odst >= 240)       bits = 32;  // Class E
        else if (*odst >= 224)  bits = 8;   // Class D
        else if (*odst >= 192)  bits = 24;  // Class C
        else if (*odst >= 128)  bits = 16;  // Class B
        else                    bits = 8;   // Class A

        // Widen if specified octets require more bits
        int octet_bits = (dst - odst) * 8;
        if (bits < octet_bits) bits = octet_bits;

        // Special case for 224.0.0.0 exactly
        if (bits == 8 && *odst == 224) bits = 4;
    }

    // Extend network to cover the actual mask
    while (bits > ((dst - odst) * 8)) {
        if (size-- <= 0U) goto emsgsize;
        *dst++ = '\0';
    }

    return bits;

enoent:
    errno = ENOENT;
    return -1;

emsgsize:
    errno = EMSGSIZE;
    return -1;
}
```
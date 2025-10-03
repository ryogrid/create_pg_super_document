# pg_sockaddr_cidr_mask

## Location
[src/backend/libpq/ifaddr.c:105-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/ifaddr.c#L105-L180)

## Overview
Creates a network mask of the appropriate address family with a specified number of significant bits for CIDR subnet calculations.

## Definition

```c
int
pg_sockaddr_cidr_mask(struct sockaddr_storage *mask, char *numbits, int family)
```
## Detailed Description
This function generates network masks for IPv4 and IPv6 addresses based on CIDR notation. It takes a string representation of the number of network bits and converts it into a properly formatted network mask for the specified address family. For IPv4, it creates a 32-bit mask, and for IPv6, it creates a 128-bit mask by setting the appropriate number of leading bits. If numbits is NULL, it creates a full mask (32 bits for IPv4, 128 bits for IPv6). The function handles bit manipulation carefully to avoid non-portable operations like shifting by 32 bits.

## Parameters / Member Variables
- `*mask`: Output parameter where the generated network mask will be stored
- `*numbits`: String representation of the number of network bits (can be NULL for full mask)
- `family`: Address family (AF_INET for IPv4, AF_INET6 for IPv6)
## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32
- Called from (representative examples):
  - [check_network_callback](../c/check_network_callback.md)
  - [parse_hba_line](parse_hba_line.md)
  - [run_ifaddr_callback](../r/run_ifaddr_callback.md)
  - IFADDR_H

## Notes and Other Information
- Returns 0 on success, -1 on error (invalid bit count or unsupported address family)
- For IPv4: accepts 0-32 bits, creates a 32-bit mask using bit shifting
- For IPv6: accepts 0-128 bits, creates a 128-bit mask by setting bytes iteratively
- Handles edge cases carefully, such as avoiding "x << 32" which is not portable
- Uses pg_hton32() to convert the IPv4 mask to network byte order
- Sets the ss_family field in the output mask structure
- Input validation includes checking that numbits contains only numeric characters

## Simplified Source

```c
// Simplified version of pg_sockaddr_cidr_mask
int pg_sockaddr_cidr_mask(struct sockaddr_storage *mask, char *numbits, int family) {
    long bits;

    // Determine number of bits: default to full mask if numbits is NULL
    if (numbits == NULL) {
        bits = (family == AF_INET) ? 32 : 128;
    } else {
        // Parse the bit count from string
        bits = strtol(numbits, &endptr, 10);
        if (*numbits == '\0' || *endptr != '\0')
            return -1;  // Invalid number format
    }

    switch (family) {
        case AF_INET:
            {
                struct sockaddr_in mask4;

                // Validate IPv4 bit range (0-32)
                if (bits < 0 || bits > 32)
                    return -1;

                // Create IPv4 mask by shifting bits
                memset(&mask4, 0, sizeof(mask4));
                if (bits > 0) {
                    long maskl = (0xffffffffUL << (32 - bits)) & 0xffffffffUL;
                    mask4.sin_addr.s_addr = pg_hton32(maskl);
                }
                memcpy(mask, &mask4, sizeof(mask4));
                break;
            }

        case AF_INET6:
            {
                struct sockaddr_in6 mask6;

                // Validate IPv6 bit range (0-128)
                if (bits < 0 || bits > 128)
                    return -1;

                // Create IPv6 mask byte by byte
                memset(&mask6, 0, sizeof(mask6));
                for (int i = 0; i < 16; i++) {
                    if (bits <= 0)
                        mask6.sin6_addr.s6_addr[i] = 0x00;      // No more bits
                    else if (bits >= 8)
                        mask6.sin6_addr.s6_addr[i] = 0xff;      // Full byte
                    else
                        mask6.sin6_addr.s6_addr[i] = (0xff << (8 - bits)) & 0xff;  // Partial byte
                    bits -= 8;
                }
                memcpy(mask, &mask6, sizeof(mask6));
                break;
            }

        default:
            return -1;  // Unsupported address family
    }

    // Set the address family and return success
    mask->ss_family = family;
    return 0;
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Clarified the logic flow with better variable naming context
- Consolidated error handling patterns
- Made the bit manipulation logic more explicit with comments
- Focused on the core algorithm while preserving all essential functionality
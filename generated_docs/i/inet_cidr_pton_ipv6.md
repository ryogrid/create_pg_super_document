# inet_cidr_pton_ipv6

## Location
[src/backend/utils/adt/inet_net_pton.c:439-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L439-L564)

## Overview
Converts IPv6 network addresses from presentation format to network format, handling CIDR notation, IPv4-mapped addresses, and size constraints.

## Definition

```c
static int
inet_cidr_pton_ipv6(const char *src, u_char *dst, size_t size)
```
## Detailed Description
This function is the core IPv6 address parser that converts IPv6 network addresses from human-readable presentation format into binary network format. It supports full IPv6 syntax including:

- Standard IPv6 colon-separated hexadecimal notation
- Compressed notation with "::", representing zero compression
- IPv4-mapped IPv6 addresses (e.g., ::ffff:192.168.1.1)
- CIDR prefix notation (e.g., 2001:db8::/32)
- Mixed case hexadecimal digits

The function performs comprehensive validation of the input format and handles various edge cases in IPv6 address parsing. It uses a finite state machine approach to parse the address character by character, building the binary representation while tracking state such as zero compression location and handling transitions between different address components.

The parser supports the special "::" notation for zero compression, which can appear anywhere in the address to represent one or more groups of zeros. It also handles IPv4 dotted-decimal notation when it appears at the end of an IPv6 address for IPv4-mapped or IPv4-compatible addresses.

## Parameters / Member Variables
- `*src`: Input string containing the IPv6 network address in presentation format
- `*dst`: Output buffer where the converted network address will be stored in binary format
- `size`: Size of the destination buffer in bytes (must be at least NS_IN6ADDRSZ = 16 bytes)
## Dependencies
- Functions called/Symbols referenced:
  - strchr (standard C library)
  - memset (standard C library) 
  - memcpy (standard C library)
  - [getv4](../g/getv4.md) (for parsing IPv4-mapped addresses)
  - [getbits](../g/getbits.md) (for parsing CIDR prefix notation)
- Constants referenced:
  - NS_IN6ADDRSZ (16 - size of IPv6 address)
  - NS_INT16SZ (2 - size of 16-bit integer)
  - NS_INADDRSZ (4 - size of IPv4 address)
  - EMSGSIZE (errno value for insufficient buffer size)
  - ENOENT (errno value for invalid input)
- Called from (representative examples):
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md)
  - [inet_net_pton_ipv6](inet_net_pton_ipv6.md)

## Notes and Other Information
- Returns the number of network bits specified by CIDR notation, or 128 if no prefix is specified
- Returns -1 on error with errno set appropriately (ENOENT for invalid format, EMSGSIZE for insufficient buffer)
- Handles the complex IPv6 zero compression (::) by using a two-pass approach: first parse into a temporary buffer, then rearrange if zero compression was used
- The function is careful to validate buffer boundaries to prevent overflow
- Supports both lowercase and uppercase hexadecimal digits
- Originally based on code from Internet Systems Consortium (ISC)
- This is a static function internal to the inet_net_pton.c file
- The parsing algorithm handles the ambiguity of :: placement by tracking the colonp pointer to mark where zero compression should be applied

## Simplified Source

```c
static int
inet_cidr_pton_ipv6(const char *src, u_char *dst, size_t size)
{
    u_char tmp[16], *tp, *endp, *colonp;
    const char *curtok;
    int ch, saw_xdigit;
    u_int val;
    int digits;
    int bits = -1;

    // Validate buffer size
    if (size < 16)
        goto emsgsize;

    // Initialize temporary buffer and pointers
    memset(tmp, 0, 16);
    tp = tmp;
    endp = tp + 16;
    colonp = NULL;

    // Handle leading "::"
    if (*src == ':')
        if (*++src != ':')
            goto enoent;

    curtok = src;
    saw_xdigit = 0;
    val = 0;
    digits = 0;

    // Parse character by character
    while ((ch = *src++) != '\0')
    {
        // Handle hexadecimal digits
        if ((ch >= '0' && ch <= '9') ||
            (ch >= 'a' && ch <= 'f') ||
            (ch >= 'A' && ch <= 'F'))
        {
            // Convert hex digit to value
            if (ch >= '0' && ch <= '9')
                val = (val << 4) | (ch - '0');
            else if (ch >= 'a' && ch <= 'f')
                val = (val << 4) | (ch - 'a' + 10);
            else
                val = (val << 4) | (ch - 'A' + 10);

            if (++digits > 4)
                goto enoent;
            saw_xdigit = 1;
            continue;
        }

        // Handle colon separator
        if (ch == ':')
        {
            curtok = src;
            if (!saw_xdigit)
            {
                // Handle "::" zero compression
                if (colonp)
                    goto enoent;
                colonp = tp;
                continue;
            }
            else if (*src == '\0')
                goto enoent;

            // Store 16-bit value
            if (tp + 2 > endp)
                goto enoent;
            *tp++ = (val >> 8) & 0xff;
            *tp++ = val & 0xff;
            saw_xdigit = 0;
            digits = 0;
            val = 0;
            continue;
        }

        // Handle IPv4 embedded address (e.g., ::ffff:192.168.1.1)
        if (ch == '.' && (tp + 4 <= endp) && getv4(curtok, tp, &bits) > 0)
        {
            tp += 4;
            saw_xdigit = 0;
            break;
        }

        // Handle CIDR prefix notation
        if (ch == '/' && getbits(src, &bits) > 0)
            break;

        goto enoent;
    }

    // Handle final hexadecimal group
    if (saw_xdigit)
    {
        if (tp + 2 > endp)
            goto enoent;
        *tp++ = (val >> 8) & 0xff;
        *tp++ = val & 0xff;
    }

    // Default to full address if no CIDR specified
    if (bits == -1)
        bits = 128;

    // Handle zero compression "::" expansion
    if (colonp != NULL)
    {
        int n = tp - colonp;
        int i;

        if (tp == endp)
            goto enoent;

        // Shift bytes to make room for zeros
        for (i = 1; i <= n; i++)
        {
            endp[-i] = colonp[n - i];
            colonp[n - i] = 0;
        }
        tp = endp;
    }

    // Validate complete address
    if (tp != endp)
        goto enoent;

    // Copy result to destination
    memcpy(dst, tmp, 16);
    return bits;

enoent:
    errno = ENOENT;
    return -1;

emsgsize:
    errno = EMSGSIZE;
    return -1;
}
```
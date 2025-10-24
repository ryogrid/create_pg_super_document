# inet_net_ntop_ipv6

## Location
[src/port/inet_net_ntop.c:178-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/inet_net_ntop.c#L178-L296)

## Overview
Converts IPv6 network addresses from binary network format to presentation format with CIDR notation, implementing RFC-compliant IPv6 address compression and embedded IPv4 handling.

## Definition

```c
struct
	{
		int			base,
					len;
	}			best, cur;
```
## Detailed Description
This static function handles IPv6-specific conversion from 16-byte binary format to standard IPv6 text representation. It implements several key IPv6 formatting features: zero compression (:: notation) for the longest run of consecutive zero 16-bit groups, detection and formatting of embedded IPv4 addresses (IPv4-mapped, IPv4-compatible, and 6to4 addresses), and optional CIDR prefix notation. The function uses a two-pass algorithm: first preprocessing to find optimal zero compression opportunities, then formatting the final output string.

The implementation is portable across different architectures, avoiding assumptions about integer sizes and byte ordering that might not hold on all systems.

## Parameters
- `src`: Pointer to the 16-byte IPv6 address in network byte order
- `bits`: Number of network bits for CIDR notation (-1 for no prefix, 0-128)
- `dst`: Output buffer to store the formatted string
- `size`: Size of the destination buffer

## Dependencies
- Functions called/Symbols referenced:
  - [decoct](../d/decoct.md) (for IPv4 embedded address formatting)
  - SPRINTF (macro for formatted string output)
  - NS_IN6ADDRSZ (IPv6 address size constant)
  - NS_INT16SZ (16-bit integer size constant)
  - EMSGSIZE (error constant for insufficient buffer size)
  - EINVAL (error constant for invalid input)
- Called from (representative examples):
  - [pg_inet_net_ntop](../p/pg_inet_net_ntop.md)

## Notes and Other Information
- Returns pointer to dst on success, NULL on error (check errno)
- Validates bits parameter range (-1 to 128) and returns EINVAL for invalid values
- Implements RFC-compliant zero compression with :: notation
- Detects and formats embedded IPv4 addresses in various IPv6 formats
- Omits CIDR prefix for -1 bits parameter or when bits equals 128
- Uses temporary buffer to build output, then copies to destination
- Returns EMSGSIZE error if output buffer is too small
- Portable implementation that works across different architectures

## Simplified Source

```c
static char *
inet_net_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)
{
    char tmp[sizeof "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255/128"];
    char *tp;
    struct { int base, len; } best, cur;
    u_int words[NS_IN6ADDRSZ / NS_INT16SZ];
    int i;

    // Validate bits parameter
    if ((bits < -1) || (bits > 128)) {
        errno = EINVAL;
        return NULL;
    }

    // Convert bytes to 16-bit words and find longest zero run
    memset(words, '\0', sizeof words);
    for (i = 0; i < NS_IN6ADDRSZ; i++)
        words[i / 2] |= (src[i] << ((1 - (i % 2)) << 3));

    // Find the longest run of zeros for :: compression
    best.base = -1;
    cur.base = -1;
    best.len = 0;
    cur.len = 0;

    for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++) {
        if (words[i] == 0) {
            if (cur.base == -1)
                cur.base = i, cur.len = 1;
            else
                cur.len++;
        } else {
            if (cur.base != -1) {
                if (best.base == -1 || cur.len > best.len)
                    best = cur;
                cur.base = -1;
            }
        }
    }
    if (cur.base != -1) {
        if (best.base == -1 || cur.len > best.len)
            best = cur;
    }
    if (best.base != -1 && best.len < 2)
        best.base = -1;

    // Format the IPv6 address
    tp = tmp;
    for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++) {
        // Handle zero compression with ::
        if (best.base != -1 && i >= best.base && i < (best.base + best.len)) {
            if (i == best.base)
                *tp++ = ':';
            continue;
        }

        // Add colon separator
        if (i != 0)
            *tp++ = ':';

        // Check for embedded IPv4 address
        if (i == 6 && best.base == 0 && (best.len == 6 ||
                (best.len == 7 && words[7] != 0x0001) ||
                (best.len == 5 && words[5] == 0xffff))) {
            int n = decoct(src + 12, 4, tp, sizeof tmp - (tp - tmp));
            if (n == 0) {
                errno = EMSGSIZE;
                return NULL;
            }
            tp += strlen(tp);
            break;
        }
        tp += SPRINTF((tp, "%x", words[i]));
    }

    // Handle trailing :: compression
    if (best.base != -1 && (best.base + best.len) == (NS_IN6ADDRSZ / NS_INT16SZ))
        *tp++ = ':';
    *tp = '\0';

    // Add CIDR prefix if specified
    if (bits != -1 && bits != 128)
        tp += SPRINTF((tp, "/%u", bits));

    // Check buffer size and copy result
    if ((size_t) (tp - tmp) > size) {
        errno = EMSGSIZE;
        return NULL;
    }
    strcpy(dst, tmp);
    return dst;
}
```
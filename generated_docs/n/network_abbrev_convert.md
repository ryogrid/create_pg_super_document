# network_abbrev_convert

## Location
[src/backend/utils/adt/network.c:625-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L625-L795)

## Overview
SortSupport conversion routine that transforms inet/cidr values into abbreviated keys suitable for fast integer-based comparisons during sorting operations.

## Definition
```c
static Datum network_abbrev_convert(Datum original, SortSupport ssup)
```

## Detailed Description
This function implements a sophisticated abbreviated key conversion system for inet/cidr data types within PostgreSQL's SortSupport framework. It converts network addresses into compact integer representations that preserve the sorting semantics of the original network_cmp_internal() comparison rules while enabling much faster sorting through simple 3-way unsigned integer comparisons.

The conversion follows the standard inet/cidr sorting hierarchy:
1. IPv4 addresses always sort before IPv6 addresses
2. Network bits (masked portion) are compared first
3. Netmask size is compared next
4. All bits (subnet portion) are compared last

The function generates different abbreviated key formats depending on the datum size (4 or 8 bytes) and IP family:

**IPv4 with 4-byte datums**: Stores IP family bit + 31 bits of network (1 bit truncated)
**IPv4 with 8-byte datums**: Stores IP family bit + full 32-bit network + 6-bit netmask size + 25 bits of subnet
**IPv6 with 4-byte datums**: Stores IP family bit + 31 bits of network (up to 97 bits truncated)
**IPv6 with 8-byte datums**: Stores IP family bit + 63 bits of network (up to 65 bits truncated)

The function also maintains cardinality statistics for the abbreviation abort mechanism by hashing abbreviated keys and feeding them to the HyperLogLog estimator.

## Parameters / Member Variables
- `original`: Datum containing the original inet/cidr value to be converted
- `ssup`: SortSupport structure containing conversion context and state information

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md): Extracts inet pointer from datum
  - `ip_family`: Gets IP protocol family (IPv4/IPv6)
  - `ip_addr`: Gets address bytes from inet structure
  - `ip_bits`: Gets netmask size from inet structure
  - `ip_maxbits`: Gets maximum bits for IP family (32 for IPv4, 128 for IPv6)
  - `pg_bswap32`: Byte-swaps 32-bit values on little-endian systems
  - `DatumBigEndianToNative`: Converts big-endian datum to native byte order
  - `[addHyperLogLog](../a/addHyperLogLog.md)`: Adds hash to cardinality estimator
  - [hash_uint32](../h/hash_uint32.md): Hashes 32-bit values for cardinality estimation

- Called from (representative examples):
  - [network_sortsupport](network_sortsupport.md): Sets this function as the conversion routine in sort support initialization

## Notes and Other Information
- This is a static function internal to the network.c module
- The function handles both IPv4 and IPv6 addresses with different encoding strategies
- Byte order conversion is handled explicitly for cross-platform compatibility
- The subnet size calculation uses modulo arithmetic to handle edge cases where network bits exceed datum size
- Different bit allocation schemes optimize space usage based on datum size and IP family
- Cardinality tracking is performed only when the estimation phase is active
- The abbreviated keys are designed to be compared as unsigned integers while preserving correct sort order
- Special handling prevents clobbering of the IP family bit during key assembly

## Simplified Source

```c
static Datum network_abbrev_convert(Datum original, SortSupport ssup) {
    network_sortsupport_state *uss = ssup->ssup_extra;
    inet *authoritative = DatumGetInetPP(original);
    Datum res, ipaddr_datum, subnet_bitmask, network;
    int subnet_size;

    // Extract IP address bytes with proper byte order conversion
    if (ip_family(authoritative) == PGSQL_AF_INET) {
        uint32 ipaddr_datum32;
        memcpy(&ipaddr_datum32, ip_addr(authoritative), sizeof(uint32));
        ipaddr_datum = pg_bswap32(ipaddr_datum32);  // Convert to native order
        res = (Datum) 0;  // IPv4: family bit = 0
    } else {
        memcpy(&ipaddr_datum, ip_addr(authoritative), sizeof(Datum));
        ipaddr_datum = DatumBigEndianToNative(ipaddr_datum);
        res = ((Datum) 1) << (SIZEOF_DATUM * BITS_PER_BYTE - 1);  // IPv6: family bit = 1
    }

    // Calculate network/subnet split based on netmask
    subnet_size = ip_maxbits(authoritative) - ip_bits(authoritative);
    subnet_size %= SIZEOF_DATUM * BITS_PER_BYTE;

    if (ip_bits(authoritative) == 0) {
        subnet_bitmask = ((Datum) 0) - 1;  // All bits are subnet
        network = 0;
    } else if (ip_bits(authoritative) < SIZEOF_DATUM * BITS_PER_BYTE) {
        subnet_bitmask = (((Datum) 1) << subnet_size) - 1;
        network = ipaddr_datum & ~subnet_bitmask;  // Split network/subnet
    } else {
        subnet_bitmask = 0;  // All bits are network
        network = ipaddr_datum;
    }

    // Pack abbreviated key based on datum size and IP family
#if SIZEOF_DATUM == 8
    if (ip_family(authoritative) == PGSQL_AF_INET) {
        // IPv4 8-byte: network + netmask_size + subnet bits
        Datum netmask_size = (Datum) ip_bits(authoritative);
        Datum subnet = ipaddr_datum & subnet_bitmask;

        network <<= (ABBREV_BITS_INET4_NETMASK_SIZE + ABBREV_BITS_INET4_SUBNET);
        netmask_size <<= ABBREV_BITS_INET4_SUBNET;

        if (subnet_size > ABBREV_BITS_INET4_SUBNET)
            subnet >>= subnet_size - ABBREV_BITS_INET4_SUBNET;

        res |= network | netmask_size | subnet;
    } else
#endif
    {
        // 4-byte datums or IPv6: use available network bits
        res |= network >> 1;  // Preserve family bit
    }

    // Track cardinality for abbreviation effectiveness
    uss->input_count += 1;
    if (uss->estimating) {
        uint32 tmp = (uint32) res;
#if SIZEOF_DATUM == 8
        tmp ^= (uint32) ((uint64) res >> 32);
#endif
        addHyperLogLog(&uss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
    }

    return res;
}
```
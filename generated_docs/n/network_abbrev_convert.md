# network_abbrev_convert

## Location
src/backend/utils/adt/network.c: 625 - 795

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
  - `[DatumGetInetPP](../D/DatumGetInetPP.md)`: Extracts inet pointer from datum
  - `ip_family`: Gets IP protocol family (IPv4/IPv6)
  - `ip_addr`: Gets address bytes from inet structure
  - `ip_bits`: Gets netmask size from inet structure
  - `ip_maxbits`: Gets maximum bits for IP family (32 for IPv4, 128 for IPv6)
  - `pg_bswap32`: Byte-swaps 32-bit values on little-endian systems
  - `DatumBigEndianToNative`: Converts big-endian datum to native byte order
  - `addHyperLogLog`: Adds hash to cardinality estimator
  - `[hash_uint32](../h/hash_uint32.md)`: Hashes 32-bit values for cardinality estimation

- Called from (representative examples):
  - `[network_sortsupport](network_sortsupport.md)`: Sets this function as the conversion routine in sort support initialization

## Notes and Other Information
- This is a static function internal to the network.c module
- The function handles both IPv4 and IPv6 addresses with different encoding strategies
- Byte order conversion is handled explicitly for cross-platform compatibility
- The subnet size calculation uses modulo arithmetic to handle edge cases where network bits exceed datum size
- Different bit allocation schemes optimize space usage based on datum size and IP family
- Cardinality tracking is performed only when the estimation phase is active
- The abbreviated keys are designed to be compared as unsigned integers while preserving correct sort order
- Special handling prevents clobbering of the IP family bit during key assembly
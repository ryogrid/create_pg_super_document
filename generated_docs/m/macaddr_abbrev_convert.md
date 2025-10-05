# macaddr_abbrev_convert

## Location
[src/backend/utils/adt/mac.c:483-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L483-L532)

## Overview
A SortSupport conversion function that transforms MAC address data into abbreviated key representation for optimized sorting operations by packing the 6-byte MAC address into a Datum for efficient comparison.

## Definition

```c
static Datum
macaddr_abbrev_convert(Datum original, SortSupport ssup)
```
## Detailed Description
This function serves as the core conversion routine for PostgreSQL's abbreviated key optimization when sorting MAC addresses. It transforms the original MAC address representation into a compact abbreviated form that enables faster comparisons during sort operations.

The conversion process involves several key steps:
1. **Memory Layout Optimization**: On 64-bit systems, the 6-byte MAC address is copied into an 8-byte Datum with zero padding, while on 32-bit systems it uses the available 4 bytes
2. **Cardinality Tracking**: During the estimation phase, it contributes to HyperLogLog cardinality estimation by hashing abbreviated values
3. **Endianness Normalization**: Converts the result to native byte order to ensure proper comparison behavior across different architectures

The function is designed to work with , an unsigned integer comparator that provides superior performance compared to memory-based comparison functions.

## Parameters / Member Variables
- `original`: The original MAC address value as a Datum
- `ssup`: SortSupport structure containing optimization state and configuration
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts macaddr pointer from Datum
  - : Adds value to HyperLogLog cardinality estimator
  - : Computes hash of 32-bit value for cardinality estimation
  - : Converts Datum to uint32
  - : Converts Datum to native endianness
  - : MAC address-specific sort support state
  - : Platform-specific Datum size macro
- Called from (representative examples):
  - : Sets this function as the abbreviation converter callback

## Notes and Other Information
- This is a static function internal to the MAC address data type implementation  
- The function handles both 32-bit and 64-bit architectures with conditional compilation
- On 64-bit systems, two bytes of zero padding are added to fill the 8-byte Datum
- For cardinality estimation, the function XORs the upper and lower 32-bit halves on 64-bit systems to increase entropy
- The endianness conversion ensures compatibility with unsigned integer comparison functions across different platforms
- Input count tracking supports the abbreviation effectiveness analysis performed by the abort callback

## Simplified Source

```c
static Datum
macaddr_abbrev_convert(Datum original, SortSupport ssup)
{
    macaddr_sortsupport_state *uss = ssup->ssup_extra;
    macaddr *authoritative = DatumGetMacaddrP(original);
    Datum result;

    // Pack MAC address into Datum (6 bytes with padding on 64-bit)
#if SIZEOF_DATUM == 8
    memset(&result, 0, SIZEOF_DATUM);  // Zero-pad to 8 bytes
    memcpy(&result, authoritative, sizeof(macaddr));  // Copy 6 bytes
#else
    memcpy(&result, authoritative, SIZEOF_DATUM);  // Copy 4 bytes on 32-bit
#endif

    // Track input count for statistics
    uss->input_count += 1;

    // Update cardinality estimation during estimation phase
    if (uss->estimating) {
        uint32 hash_input;
#if SIZEOF_DATUM == 8
        // XOR upper and lower halves for better entropy
        hash_input = (uint32) result ^ (uint32) ((uint64) result >> 32);
#else
        hash_input = (uint32) result;
#endif
        addHyperLogLog(&uss->abbr_card, DatumGetUInt32(hash_uint32(hash_input)));
    }

    // Convert to native byte order for efficient comparison
    result = DatumBigEndianToNative(result);

    return result;
}
```
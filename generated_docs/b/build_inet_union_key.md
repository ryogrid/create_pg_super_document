# build_inet_union_key

## Location
[src/backend/utils/adt/network_gist.c:472-504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L472-L504)

## Overview
A static helper function that constructs a GistInetKey representing the union of multiple inet values by creating a key with specified parameters and common address bits.

## Definition

```c
static GistInetKey *
build_inet_union_key(int family, int minbits, int commonbits,
					 unsigned char *addr)
```
## Detailed Description
This function creates a new GistInetKey structure that represents the union of multiple inet values by combining their common characteristics. The function takes pre-calculated union parameters (typically from calc_inet_union_params functions) and constructs a proper GiST index key.

The key construction process involves:
1. **Memory allocation**: Uses palloc0() to allocate and zero-initialize the key structure
2. **Parameter assignment**: Sets the family, minbits, and commonbits fields directly
3. **Address copying**: Copies only the common address bits from the source address
4. **Bit masking**: Clears any unused bits in the final partial byte to ensure clean data
5. **Header setting**: Properly sets the varlena header for the key structure

The function ensures data integrity by masking unused bits in partial bytes and properly initializing the PostgreSQL varlena header structure.

## Parameters / Member Variables
- `family`: IP address family number (0 for mixed families, AF_INET/AF_INET6 for specific families)
- `minbits`: Minimum netmask width among the represented values
- `commonbits`: Number of leading address bits common to all represented values
- `*addr`: Pointer to address bytes from any of the union input keys (only common bits matter)
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - gk_ip_family, gk_ip_minbits, gk_ip_commonbits, gk_ip_addr (accessor macros)
  - memcpy
  - SET_GK_VARSIZE
- Called from (representative examples):
  - [inet_gist_union](../i/inet_gist_union.md)
  - [inet_gist_picksplit](../i/inet_gist_picksplit.md) (multiple calls during page splitting)

## Notes and Other Information
- This is a static function, only accessible within network_gist.c
- Uses palloc0() to ensure all bytes start as zero, preventing garbage data
- The addr parameter can be from any input key since only common bits are copied
- Bit masking in partial bytes ensures deterministic key comparison behavior
- Essential for GiST index structure maintenance and page splitting operations
- The returned key represents the tightest possible bounding box for the input inet values

## Simplified Source

```c
static GistInetKey *
build_inet_union_key(int family, int minbits, int commonbits,
                     unsigned char *addr)
{
    GistInetKey *result;

    // Allocate and zero-initialize the key structure
    result = (GistInetKey *) palloc0(sizeof(GistInetKey));

    // Set the union parameters
    gk_ip_family(result) = family;
    gk_ip_minbits(result) = minbits;
    gk_ip_commonbits(result) = commonbits;

    // Copy common address bits if any exist
    if (commonbits > 0)
        memcpy(gk_ip_addr(result), addr, (commonbits + 7) / 8);

    // Clean unwanted bits in the last partial byte
    if (commonbits % 8 != 0)
        gk_ip_addr(result)[commonbits / 8] &= ~(0xFF >> (commonbits % 8));

    // Set proper varlena header
    SET_GK_VARSIZE(result);

    return result;
}
```
# bitncmp

## Location
[src/backend/utils/adt/network.c:1569-1602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1569-L1602)

## Overview
Compares two bit masks for a specified number of bits, returning a comparison result in libc tradition.

## Definition
```c
int bitncmp(const unsigned char *l, const unsigned char *r, int n)
```

## Detailed Description
This function performs bit-level comparison between two byte arrays (bit masks) for exactly n bits. It first compares complete bytes using memcmp(), then handles any remaining bits by comparing them individually from most significant to least significant. The function assumes network byte order and returns negative, zero, or positive values following libc comparison conventions. Originally authored by Paul Vixie (ISC) in June 1996, this function is essential for network address and subnet operations.

## Parameters / Member Variables
- `l`: Pointer to the left bit mask (unsigned char array)
- `r`: Pointer to the right bit mask (unsigned char array)  
- `n`: Number of bits to compare

## Dependencies
- Functions called/Symbols referenced:
  - `memcmp` - standard C library function for byte comparison
  - `IS_HIGHBIT_SET` - macro to check if the high bit of a byte is set
- Called from (representative examples):
  - [network_cmp_internal](../n/network_cmp_internal.md) - internal network comparison function
  - [network_sub](../n/network_sub.md) - subnet containment check
  - [network_subeq](../n/network_subeq.md) - subnet containment or equality check
  - [network_sup](../n/network_sup.md) - supernet containment check
  - [network_supeq](../n/network_supeq.md) - supernet containment or equality check
  - [network_overlap](../n/network_overlap.md) - network overlap check
  - [inet_gist_consistent](../i/inet_gist_consistent.md) - GiST index consistency check
  - [inet_spg_choose](../i/inet_spg_choose.md) - SP-GiST index choose function

## Notes and Other Information
- Returns <0 if left mask is less than right mask, >0 if greater, 0 if equal
- Assumes network byte order (most significant byte first)
- Efficiently handles both complete byte comparisons and partial bit comparisons
- Critical for implementing network containment, overlap, and ordering operations
- Example: comparing 192.5.5.240/28 involves comparing 0x11110000 in the fourth octet
- Located in src/backend/utils/adt/network.c:1569-1602
- Used extensively throughout PostgreSQL's network indexing and comparison functions

## Simplified Source

```c
int bitncmp(const unsigned char *left_mask, const unsigned char *right_mask, int num_bits) {
    // Compare complete bytes first using standard library
    int complete_bytes = num_bits / 8;
    int byte_comparison = memcmp(left_mask, right_mask, complete_bytes);

    // If bytes differ or no remaining bits, return result
    if (byte_comparison != 0 || (num_bits % 8) == 0) {
        return byte_comparison;
    }

    // Compare remaining bits individually from most significant bit
    unsigned char left_byte = left_mask[complete_bytes];
    unsigned char right_byte = right_mask[complete_bytes];

    for (int remaining_bits = num_bits % 8; remaining_bits > 0; remaining_bits--) {
        // Check if high bits differ
        bool left_has_high_bit = IS_HIGHBIT_SET(left_byte);
        bool right_has_high_bit = IS_HIGHBIT_SET(right_byte);

        if (left_has_high_bit != right_has_high_bit) {
            return left_has_high_bit ? 1 : -1;
        }

        // Shift both bytes left to check next bit
        left_byte <<= 1;
        right_byte <<= 1;
    }

    return 0; // All compared bits are equal
}
```
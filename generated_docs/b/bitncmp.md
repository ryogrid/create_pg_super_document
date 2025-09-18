# bitncmp

## Location
src/backend/utils/adt/network.c: 1569 - 1602

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
  - `network_cmp_internal` - internal network comparison function
  - `network_sub` - subnet containment check
  - `network_subeq` - subnet containment or equality check
  - `network_sup` - supernet containment check
  - `network_supeq` - supernet containment or equality check
  - `network_overlap` - network overlap check
  - `inet_gist_consistent` - GiST index consistency check
  - `inet_spg_choose` - SP-GiST index choose function

## Notes and Other Information
- Returns <0 if left mask is less than right mask, >0 if greater, 0 if equal
- Assumes network byte order (most significant byte first)
- Efficiently handles both complete byte comparisons and partial bit comparisons
- Critical for implementing network containment, overlap, and ordering operations
- Example: comparing 192.5.5.240/28 involves comparing 0x11110000 in the fourth octet
- Located in src/backend/utils/adt/network.c:1569-1602
- Used extensively throughout PostgreSQL's network indexing and comparison functions
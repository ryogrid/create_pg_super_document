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
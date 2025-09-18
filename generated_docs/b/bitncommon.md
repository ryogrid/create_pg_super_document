# bitncommon

## Location
src/backend/utils/adt/network.c: 1603 - 1640

## Overview
Calculates the number of leading bits that are common between two bit masks, up to a specified maximum number of bits.

## Definition
```c
int bitncommon(const unsigned char *l, const unsigned char *r, int n)
```

## Detailed Description
This function compares two bit masks byte by byte and bit by bit to determine how many consecutive leading bits match between them. It first compares complete bytes for exact equality, and when it finds the first differing byte, it examines the individual bits within that byte from most significant to least significant to count exactly how many leading bits are common. This is essential for determining the longest common prefix between network addresses, which is used in CIDR operations and network indexing.

## Parameters / Member Variables
- `l`: Pointer to the left bit mask (unsigned char array)
- `r`: Pointer to the right bit mask (unsigned char array)
- `n`: Maximum number of bits to examine

## Dependencies
- Functions called/Symbols referenced:
  - Uses only standard C operations (XOR, bit shifting, modulo, division)
- Called from (representative examples):
  - [inet_merge](../i/inet_merge.md) - for calculating the common prefix when merging networks
  - [calc_inet_union_params](../c/calc_inet_union_params.md) - in GiST index union calculations
  - [inet_gist_penalty](../i/inet_gist_penalty.md) - for GiST index penalty calculations
  - [inet_spg_choose](../i/inet_spg_choose.md) - in SP-GiST index choose operations
  - [inet_spg_picksplit](../i/inet_spg_picksplit.md) - in SP-GiST index split operations
  - [inet_hist_match_divider](../i/inet_hist_match_divider.md) - for histogram matching in selectivity estimation

## Notes and Other Information
- Returns a value from 0 to n indicating the number of matching leading bits
- Efficiently handles both complete byte comparisons and partial bit analysis
- Uses XOR operation to identify differing bits in the final partial byte
- Critical for network prefix calculations, CIDR merging, and indexing operations
- The algorithm stops at the first differing bit to return the exact common prefix length
- Located in src/backend/utils/adt/network.c:1603-1640
- Widely used in PostgreSQL's network indexing infrastructure (GiST, SP-GiST) and query planning
# inet_gist_picksplit

## Location
src/backend/utils/adt/network_gist.c: 663 - 796

## Overview
The GiST page split function for inet data type that determines how to distribute index entries across two pages when a page becomes full during index operations.

## Definition
```c
Datum inet_gist_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the core splitting strategy for GiST indexes on inet values. When a GiST page becomes full and needs to be split, this function decides how to distribute the existing entries plus a new entry across two new pages to maintain index efficiency and preserve spatial locality.

The function employs a sophisticated two-tier splitting strategy:

**1. IP Family-based Splitting (Priority 1):**
If entries contain different IP families (IPv4 vs IPv6), the split is performed along family boundaries. This ensures that IPv4 and IPv6 addresses are kept in separate index subtrees, optimizing queries that target specific IP families.

**2. Address-based Splitting (Priority 2):**
When all entries are the same IP family, the function analyzes the bit patterns of the addresses:
- Calculates the number of leading bits common to all addresses
- Attempts to split on the next bit position after the common prefix
- If a bit position doesn't yield a balanced split, tries the next bit position
- Falls back to an arbitrary 50-50 split if no bit position works well

After determining the split, the function computes optimized union keys for both resulting pages, ensuring that the index maintains good clustering properties and minimal coverage.

## Parameters / Member Variables
- `entryvec`: GistEntryVector containing all entries to be split
- `splitvec`: GIST_SPLITVEC structure to store the split result including left/right entry assignments and union keys

## Dependencies
- Functions called/Symbols referenced:
  - [calc_inet_union_params](../c/calc_inet_union_params.md): Analyzes union parameters for a set of inet entries
  - [calc_inet_union_params_indexed](../c/calc_inet_union_params_indexed.md): Analyzes union parameters for indexed entries
  - [build_inet_union_key](../b/build_inet_union_key.md): Constructs a union key from parameters
  - DatumGetInetKeyP: Extracts GistInetKey from Datum
  - gk_ip_family: Gets IP family from GistInetKey
  - gk_ip_addr: Gets address buffer from GistInetKey
  - ip_family_maxbits: Gets maximum bits for IP family (32 for IPv4, 128 for IPv6)
  - FirstOffsetNumber/OffsetNumberNext: Index iteration macros
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function

- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in operator class)

## Notes and Other Information
- This function is critical for maintaining good index performance as it determines the logical organization of the index tree
- The family-first splitting strategy prevents inefficient mixed-family queries
- The bit-level analysis ensures geographically or topologically related IP addresses cluster together
- The fallback 50-50 split handles edge cases where address patterns don't provide natural split points
- Union key optimization after splitting ensures minimal index key coverage and efficient query processing
- The algorithm doesn't currently consider netmask widths in address-based splitting, which is noted as a potential enhancement
- File location: src/backend/utils/adt/network_gist.c:663-796
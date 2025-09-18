# inet_gist_same

## Location
[src/backend/utils/adt/network_gist.c:797-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L797-L810)

## Overview
The GiST equality function for inet data type that determines whether two GistInetKey values represent identical index keys.

## Definition
```c
Datum inet_gist_same(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality test for GistInetKey structures in the GiST indexing system. It serves as the "same" method in the GiST operator class, which is used by the index maintenance algorithms to determine if two index keys are identical.

The function performs a comprehensive comparison of all components that define a GistInetKey:
1. **IP Family**: Both keys must have the same IP family (IPv4 or IPv6)
2. **Minimum Bits**: Both keys must have the same minimum network mask bits
3. **Common Bits**: Both keys must have the same number of common address bits  
4. **Address Data**: The actual address bytes must match exactly

This equality test is crucial for GiST operations such as detecting duplicate keys, merging identical subtrees, and optimizing index structure. The function ensures that two keys are considered identical only when they represent the exact same network address space coverage.

## Parameters / Member Variables
- `left`: GistInetKey pointer to the first key for comparison
- `right`: GistInetKey pointer to the second key for comparison  
- `result`: bool pointer to store the equality result (true if keys are identical)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInetKeyP: Extracts GistInetKey pointer from Datum
  - gk_ip_family: Gets IP family from GistInetKey
  - gk_ip_minbits: Gets minimum network bits from GistInetKey
  - gk_ip_commonbits: Gets common address bits from GistInetKey
  - gk_ip_addr: Gets address buffer from GistInetKey
  - gk_ip_addrsize: Gets address size from GistInetKey
  - memcmp: Standard C library memory comparison function

- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in operator class)

## Notes and Other Information
- This function is essential for GiST index integrity and optimization operations
- The comparison is done component-by-component for efficiency and clarity
- Address comparison uses memcmp for byte-level accuracy across the full address size
- The function handles both IPv4 (4-byte) and IPv6 (16-byte) addresses correctly through gk_ip_addrsize
- All fields must match exactly for the keys to be considered identical - partial matches return false
- Used internally by GiST algorithms for tree balancing, deduplication, and consistency checks
- File location: src/backend/utils/adt/network_gist.c:797-810
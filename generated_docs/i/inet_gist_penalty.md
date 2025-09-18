# inet_gist_penalty

## Location
src/backend/utils/adt/network_gist.c: 620 - 662

## Overview
The GiST penalty function for inet data type that calculates the cost of adding a new inet value to an existing index page during page splits.

## Definition
```c
Datum inet_gist_penalty(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a critical component of the GiST indexing strategy for inet values. It calculates a penalty score that represents the "cost" of adding a new inet value to an existing index page. The GiST algorithm uses this penalty to decide where to place new index entries, aiming to minimize index degradation and maintain good clustering.

The penalty calculation follows a hierarchical strategy:
1. **Address family mismatch**: Highest penalty (4.0) - IPv4 and IPv6 should not be mixed
2. **Network mask degradation**: High penalty (3.0) - when the new value would reduce the minimum network bits
3. **Address space expansion**: Medium penalty (2.0) - when addresses share no common bits
4. **Good clustering**: Low penalty (1.0/commonbits) - inversely proportional to shared address bits

This approach ensures that IPv4 and IPv6 addresses are kept separate, networks with similar mask sizes are grouped together, and geographically or topologically related IP addresses cluster efficiently.

## Parameters / Member Variables
- `origent`: GISTENTRY pointer to the existing index page union key
- `newent`: GISTENTRY pointer to the new inet value being inserted  
- `penalty`: float pointer to store the calculated penalty score

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInetKeyP: Extracts GistInetKey from Datum
  - gk_ip_family: Gets IP family from GistInetKey
  - gk_ip_minbits: Gets minimum network bits from GistInetKey
  - gk_ip_commonbits: Gets common address bits from GistInetKey
  - gk_ip_addr: Gets address buffer from GistInetKey
  - bitncommon: Calculates number of common leading bits between two addresses
  - Min: Macro for minimum of two values

- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in operator class)

## Notes and Other Information
- The penalty function is crucial for maintaining good index performance by ensuring logical clustering
- Lower penalties encourage similar addresses to be grouped together, improving range query performance  
- The hierarchical penalty system prioritizes IP family consistency over network size consistency
- The inverse relationship with common bits means addresses with more shared prefix bits are cheaper to group
- This function directly impacts index build time and query performance for network operations
- File location: src/backend/utils/adt/network_gist.c:620-662
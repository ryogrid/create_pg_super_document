# calc_inet_union_params

## Location
[src/backend/utils/adt/network_gist.c:345-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L345-L406)

## Overview
A static helper function that calculates parameters for the union of multiple GistInetKey entries, determining the combined characteristics of a set of inet index keys.

## Definition


## Detailed Description
This function analyzes a range of GISTENTRY elements (from index m to n inclusive) and computes four key union parameters that describe the collective characteristics of the inet keys:

1. **Family range**: Determines the minimum and maximum IP address family numbers (IPv4=AF_INET, IPv6=AF_INET6) across all keys
2. **Minimum netmask bits**: Finds the smallest netmask width among all keys 
3. **Common address bits**: Calculates the number of leading address bits that are identical across all keys

The function enforces a critical constraint: when multiple address families are present, both minbits and commonbits are forced to zero, reflecting that mixed-family keys cannot have meaningful bit-level commonality.

The algorithm initializes using the first key's values, then iterates through remaining keys to find minimums and compute bit-level commonality using the bitncommon() function.

## Parameters / Member Variables
- : Array of GISTENTRY elements containing inet keys to analyze
- : Starting index in the array (inclusive)
- : Ending index in the array (inclusive) 
- : Output parameter for minimum IP address family number
- : Output parameter for maximum IP address family number
- : Output parameter for minimum netmask width
- : Output parameter for number of common address bits

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInetKeyP
  - gk_ip_family, gk_ip_minbits, gk_ip_commonbits, gk_ip_addr
  - [bitncommon](../b/bitncommon.md)
  - Assert
- Called from (representative examples):
  - [inet_gist_union](../i/inet_gist_union.md)
  - [inet_gist_picksplit](../i/inet_gist_picksplit.md)

## Notes and Other Information
- This is a static function, only accessible within network_gist.c
- Requires at least one key (m <= n assertion)
- The mixed-family constraint ensures that union keys represent valid inet ranges
- Used during GiST index construction and maintenance operations
- Essential for maintaining index structure integrity in inet GiST indexes
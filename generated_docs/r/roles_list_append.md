# roles_list_append

## Location
src/backend/utils/adt/acl.c: 4959 - 5018

## Overview
A helper function that provides an optimized implementation of list_append_unique_oid() using a Bloom filter to efficiently manage role membership lists during privilege checking.

## Definition


## Detailed Description
This function optimizes the process of adding unique role OIDs to a list by leveraging a Bloom filter for fast membership testing. It's designed to work with roles_is_member_of() to efficiently manage role hierarchies without duplicate entries.

The function implements a two-tier optimization strategy:
1. If a Bloom filter exists, it first checks if the role is definitely absent (bloom_lacks_element returns true)
2. Only if the Bloom filter suggests the role might be present does it fall back to a linear search through the actual list

When the role list exceeds ROLES_LIST_BLOOM_THRESHOLD, the function automatically creates and populates a Bloom filter to accelerate future operations.

## Parameters / Member Variables
- : The existing list of role OIDs to potentially append to
- : Double pointer to a Bloom filter used for fast membership testing; may be NULL initially
- : The role OID to add to the list if not already present

## Dependencies
- Functions called/Symbols referenced:
  - bloom_filter (data structure)
  - bloom_lacks_element
  - list_member_oid  
  - bloom_create
  - bloom_add_element
  - lappend_oid
  - foreach_oid
  - ROLES_LIST_BLOOM_THRESHOLD (constant)
- Called from:
  - roles_is_member_of

## Notes and Other Information
- The function is marked as  for performance optimization
- Uses work_mem for Bloom filter memory allocation 
- The Bloom filter threshold is set to 10x the list length threshold for optimal space/time tradeoff
- Caller (roles_is_member_of) is responsible for freeing the Bloom filter after use
- Implements probabilistic optimization: Bloom filter false negatives are impossible, but false positives require fallback to linear search
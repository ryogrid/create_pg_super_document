# dsm_pin_mapping

## Location
src/backend/storage/ipc/dsm.c: 915 - 933

## Overview
Prevents a dynamic shared memory mapping from being automatically released when the current resource owner is cleaned up, effectively pinning it until the end of the session.

## Definition


## Detailed Description
The dsm_pin_mapping function modifies the resource ownership of a dynamic shared memory segment mapping to ensure it persists beyond the current query context. By default, DSM mappings are owned by the current resource owner, which typically means they are automatically released at the end of the current query. This function removes the segment from resource owner tracking by calling ResourceOwnerForgetDSM() and setting the resowner field to NULL, effectively transferring ownership to the session level.

This is particularly useful when a DSM segment needs to be shared across multiple queries within the same session, or when the segment contains data that should persist beyond individual transaction boundaries.

## Parameters / Member Variables
- : Pointer to the dsm_segment structure representing the dynamic shared memory segment to be pinned

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForgetDSM
  - dsm_segment (structure type)
- Called from (representative examples):
  - GetSessionDsmHandle (src/backend/access/common/session.c:139)
  - AttachSession (src/backend/access/common/session.c:187)
  - GetNamedDSMSegment (src/backend/storage/ipc/dsm_registry.c:165, 190)
  - dsa_pin_mapping (src/backend/utils/mmgr/dsa.c:645)

## Notes and Other Information
- This function only acts if the segment currently has a resource owner (seg->resowner != NULL)
- Once pinned, the mapping will remain valid until the session ends or dsm_unpin_mapping is called
- This is a critical function for implementing persistent shared data structures that span multiple queries
- The function is safe to call multiple times on the same segment (it will have no effect if already pinned)
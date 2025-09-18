# inprogressent

## Location
src/backend/utils/cache/relcache.c: 164 - 167

## Overview
The  structure tracks ongoing  calls to handle concurrent index creation and ensure proper invalidation processing during relation cache building.

## Definition


## Detailed Description
The  structure is used as part of PostgreSQL's relation cache invalidation system, specifically to handle the complex case of . This structure maintains a stack () of relations currently being built by . The key challenge it addresses is that  makes catalog changes under , and it critically relies on each backend absorbing those changes no later than the next transaction start. To ensure this,  loops until it finishes without accepting a relevant invalidation, unlike most other invalidation consumers. Each entry tracks both the relation being built and whether an invalidation message has arrived for it during the build process.

## Parameters / Member Variables
- : The object identifier (OID) of the relation currently being built by 
- : A boolean flag indicating whether an invalidation message has been received for this relation during the build process

## Dependencies
- Functions called/Symbols referenced:
  - Oid (built-in type)
  - [bool](../b/bool.md) (built-in type)
- Called from (representative examples):
  - Used by  stack management
  - Referenced during  processing
  - Used in invalidation handling logic

## Notes and Other Information
- This structure is part of the internal implementation for handling concurrent index creation scenarios
- The associated  is a stack that grows and shrinks as  calls are nested
- Critical for ensuring that  operations work correctly across multiple backends
- The invalidation tracking helps prevent race conditions where catalog changes might be missed during relation building
- Defined in  starting at line 164
- Works in conjunction with  and  variables to manage the dynamic stack
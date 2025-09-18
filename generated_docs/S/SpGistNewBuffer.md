# SpGistNewBuffer

## Location
src/backend/access/spgist/spgutils.c: 386 - 441

## Overview
Allocates a new buffer page for a SP-GiST index, either by recycling a free page from the Free Space Map (FSM) or by extending the index file with a new page.

## Definition
Buffer SpGistNewBuffer(Relation index)

## Detailed Description
This function implements a two-stage strategy for obtaining new buffer pages in SP-GiST indexes. First, it attempts to recycle existing pages by querying the Free Space Map (FSM) for available pages. For each candidate page from FSM, it performs validation checks to ensure the page is suitable for reuse: it must not be a fixed system page, must be successfully lockable, and must be either uninitialized, explicitly deleted, or empty.

If no suitable page is found through FSM recycling, the function falls back to extending the index file by allocating a completely new page. The returned buffer is already pinned and exclusively locked, ready for the caller to initialize through SpGistInitBuffer.

## Parameters / Member Variables
- : Relation object representing the SP-GiST index that needs a new buffer page

## Dependencies
- Functions called/Symbols referenced:
  - GetFreeIndexPage
  - SpGistBlockIsFixed
  - ReadBuffer
  - ConditionalLockBuffer
  - BufferGetPage
  - PageIsNew
  - SpGistPageIsDeleted
  - PageIsEmpty
  - LockBuffer
  - ReleaseBuffer
  - ExtendBufferedRel
  - BMR_REL
- Called from (representative examples):
  - spgbuild
  - allocNewBuffer

## Notes and Other Information
The function includes important concurrency control: it uses ConditionalLockBuffer to avoid blocking when another process might already be recycling the same page. Fixed system pages are explicitly excluded from recycling to maintain index structural integrity. The caller is responsible for initializing the returned buffer page content using SpGistInitBuffer before use.
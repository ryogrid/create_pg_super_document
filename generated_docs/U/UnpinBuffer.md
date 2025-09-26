# UnpinBuffer

## Location
src/backend/storage/buffer/bufmgr.c: 2795 - 2803

## Overview
UnpinBuffer decrements the pin count of a shared buffer and removes it from the current resource owner's buffer tracking, making the buffer available for potential replacement.

## Definition


## Detailed Description
UnpinBuffer is a static function that serves as a wrapper around UnpinBufferNoOwner while also handling resource ownership tracking. It specifically deals with shared buffers (never local ones) and always adjusts the CurrentResourceOwner by removing the buffer from its tracked resources before calling UnpinBufferNoOwner to perform the actual unpinning operation. This ensures proper resource management and prevents resource leaks in PostgreSQL's buffer management system.

## Parameters / Member Variables
- : Pointer to the BufferDesc structure representing the buffer to be unpinned

## Dependencies
- Functions called/Symbols referenced:
  - BufferDescriptorGetBuffer
  - ResourceOwnerForgetBuffer
  - UnpinBufferNoOwner
- Called from (representative examples):
  - BufferAlloc
  - GetVictimBuffer
  - ReleaseBuffer
  - SyncOneBuffer
  - FlushRelationBuffers

## Notes and Other Information
- This function should only be applied to shared buffers, never local ones
- Always adjusts CurrentResourceOwner to maintain proper resource tracking
- Acts as a resource-aware wrapper around the lower-level UnpinBufferNoOwner function
- Critical for preventing buffer pin leaks in PostgreSQL's memory management system
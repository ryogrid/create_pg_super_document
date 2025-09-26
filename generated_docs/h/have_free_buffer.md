# have_free_buffer

## Location
src/backend/storage/buffer/freelist.c: 175 - 195

## Overview
have_free_buffer provides a lockless check to determine if there are any free buffers available in the buffer pool without acquiring locks.

## Definition


## Detailed Description
have_free_buffer performs a simple, non-blocking check of the StrategyControl->firstFreeBuffer field to determine if there are free buffers available in the buffer pool. The function returns true if firstFreeBuffer is non-negative (indicating at least one free buffer exists), and false otherwise.

This is designed as a lightweight, lockless operation that can be used for quick availability checks without the overhead of acquiring locks. However, the result is inherently racy - by the time the caller acts on a true result, other processes may have already claimed the free buffers, making the information stale.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - StrategyControl (global buffer strategy control structure)
  - BufferDesc (buffer descriptor structure)
- Called from (representative examples):
  - ResourceOwnerForgetBufferIO

## Notes and Other Information
- The function is lockless for performance but provides no guarantees about buffer availability by the time the caller acts
- Callers that strictly require a free buffer should not rely solely on this function due to race conditions
- Primarily useful for heuristic decisions or optimizations where approximate information is sufficient
- The result becomes stale immediately after returning if other processes are concurrently allocating buffers
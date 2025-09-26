# LimitAdditionalLocalPins

## Location
src/backend/storage/buffer/localbuf.c: 290 - 312

## Overview
LimitAdditionalLocalPins limits the number of additional local buffer pins that can be acquired, serving as the local buffer equivalent of LimitAdditionalPins for temporary relations.

## Definition


## Detailed Description
LimitAdditionalLocalPins implements resource management for local buffer pins by constraining the number of additional pins that can be acquired based on available local buffer capacity. Unlike its shared buffer counterpart (LimitAdditionalPins), this function only needs to consider the local backend's buffer usage since local buffers are not shared across backends.

The function calculates the maximum allowable additional pins by subtracting the currently pinned local buffers (NLocalPinnedBuffers) from the total configured temporary buffer count (num_temp_buffers). If the requested additional pins exceed this limit, the function reduces the request to the maximum safe amount to prevent buffer exhaustion.

The function includes an optimization for small requests, immediately returning for requests of 1 or fewer additional pins since these pose minimal risk of resource exhaustion.

## Parameters
- : Pointer to the requested number of additional pins; modified in-place to the allowed limit if necessary

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only global variables num_temp_buffers and NLocalPinnedBuffers)
- Called from (representative examples):
  - read_stream_begin_relation: Used in streaming read operations for temporary relations
  - ExtendBufferedRelLocal: Used when extending buffered local relations
  - RelationGetNumberOfBlocks: Used in relation block count operations

## Notes and Other Information
- Simpler than shared buffer equivalent since no inter-backend coordination is required
- Uses num_temp_buffers instead of NLocBuffer to handle cases where local buffers aren't initialized yet
- Provides fast path for small pin requests (≤1) to avoid unnecessary computation
- Part of PostgreSQL's resource management system preventing local buffer exhaustion
- Critical for operations that may pin many buffers simultaneously, such as bulk data operations
- Helps maintain system stability by preventing runaway buffer pin acquisition in temporary relation operations
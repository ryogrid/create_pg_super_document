# shm_toc_attach

## Location
[src/backend/storage/ipc/shm_toc.c:64-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_toc.c#L64-L87)

## Overview
Attaches to an existing shared memory table of contents by validating the magic number and returning a pointer to the TOC structure if valid.

## Definition
```c
shm_toc *shm_toc_attach(uint64 magic, void *address)
```

## Detailed Description
The `shm_toc_attach` function provides a mechanism for processes to connect to an already-initialized shared memory table of contents. This is typically used by worker processes or secondary processes that need to access shared memory segments created by a primary process.

The function performs validation by checking that the magic number stored in the shared memory matches the expected magic number passed as a parameter. This validation serves multiple purposes:
- Ensures the shared memory region contains a valid TOC structure
- Prevents accidental access to unrelated memory regions  
- Provides a basic corruption detection mechanism
- Allows different types of shared memory regions to coexist

If the magic number validation fails, the function returns NULL, indicating that the shared memory region is either invalid, corrupted, or not the expected type.

The function also includes assertion checks to verify internal consistency of the TOC structure, ensuring that allocated bytes don't exceed total bytes and that the total size is sufficient for the basic TOC structure.

## Parameters / Member Variables
- `magic`: Expected 64-bit magic number that should match the value stored in the TOC
- `address`: Pointer to the shared memory region containing the existing TOC structure

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc](shm_toc.md) (structure type for casting and validation)

- Called from (representative examples):
  - [AttachSession](../A/AttachSession.md) (src/backend/access/common/session.c:170)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (src/backend/access/transam/parallel.c:1345)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md) (src/backend/replication/logical/applyparallelworker.c:892)
  - [test_shm_mq_main](../t/test_shm_mq_main.md) (src/test/modules/test_shm_mq/worker.c:85)

## Notes and Other Information
- This function is the counterpart to shm_toc_create and is used in master-worker process scenarios
- The magic number validation is crucial for preventing crashes due to memory layout mismatches
- Returning NULL on magic mismatch allows calling code to gracefully handle invalid shared memory regions
- The function performs minimal work and is designed to be fast, as it's often called during process initialization
- Assertion checks are only active in debug builds and help catch programming errors during development
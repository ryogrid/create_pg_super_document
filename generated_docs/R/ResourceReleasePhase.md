# ResourceReleasePhase

## Location
[src/include/utils/resowner.h:57-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/resowner.h#L57-L58)

## Overview
ResourceReleasePhase is an enumeration that defines the three distinct phases during which resources are released in PostgreSQL resource management system.

## Definition
```c
typedef enum
{
    RESOURCE_RELEASE_BEFORE_LOCKS = 1,
    RESOURCE_RELEASE_LOCKS,
    RESOURCE_RELEASE_AFTER_LOCKS,
} ResourceReleasePhase;
```

## Detailed Description
ResourceReleasePhase defines the temporal ordering of resource cleanup during transaction end or resource owner release. The three-phase approach ensures that resources are released in a safe order that prevents deadlocks and maintains consistency.

The phases are designed around lock management, which is central to PostgreSQL concurrency control:
- BEFORE_LOCKS: Resources that must be released before any locks are released, typically resources that are visible to other backends
- LOCKS: The lock release phase itself
- AFTER_LOCKS: Backend-internal cleanup that can occur after locks have been released

This phased approach ensures that when PostgreSQL releases a lock that another backend might be waiting for, the releasing backend has already made itself "fully out of the transaction" by releasing visible resources.

## Parameters / Member Variables
- `RESOURCE_RELEASE_BEFORE_LOCKS = 1`: Resources released before lock release; includes resources visible to other backends such as pinned buffers, relation cache references, DSM segments, JIT contexts, and crypto contexts
- `RESOURCE_RELEASE_LOCKS`: The lock release phase; locks themselves are released during this phase
- `RESOURCE_RELEASE_AFTER_LOCKS`: Resources released after locks; includes backend-internal cleanup such as catalog cache references, plan cache references, tuple descriptor references, snapshot references, files, and wait event sets

## Dependencies
- Functions called/Symbols referenced:
  - (none - this is a basic enum type)
- Called from (representative examples):
  - ResourceOwnerRelease
  - ResourceOwnerReleaseAll 
  - ResourceOwnerReleaseInternal
  - ResourceOwnerDesc (as a member field)
  - Test modules for resource management

## Notes and Other Information
- The numeric value 1 for RESOURCE_RELEASE_BEFORE_LOCKS ensures that the enum values are non-zero, which can be useful for validation
- Each phase has its own priority system defined by ResourceReleasePriority constants
- The phase separation is critical for avoiding circular dependencies during resource cleanup
- Extensions that define custom resource types must specify which phase their resources should be released in
- The design prevents scenarios where releasing one resource could require accessing another resource that has already been released
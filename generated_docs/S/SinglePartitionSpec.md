# SinglePartitionSpec

## Location
[src/include/nodes/parsenodes.h:945-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L945-L948)

## Overview
SinglePartitionSpec is a legacy stub structure maintained for NodeTag ABI compatibility, previously used in reverted ALTER TABLE SPLIT PARTITION commands.

## Definition

```c
typedef struct SinglePartitionSpec
{
	NodeTag		type;
}			SinglePartitionSpec;
```
## Detailed Description
SinglePartitionSpec is essentially a deprecated structure that has been reduced to a minimal stub containing only the required NodeTag field. According to the source comments, it was originally used in ALTER TABLE SPLIT PARTITION functionality that was later reverted, but the structure definition is kept to maintain Application Binary Interface (ABI) compatibility with existing code that might reference this node type.

This represents a common pattern in PostgreSQL development where functionality may be removed but the underlying data structures are preserved as stubs to prevent breaking changes in systems that have already compiled against the previous interface.

## Parameters / Member Variables
- : Standard NodeTag for the PostgreSQL node system, the only remaining field in this stub structure

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited)
- Called from (representative examples):
  - No current references (legacy stub)

## Notes and Other Information
- This is a legacy structure maintained solely for ABI compatibility
- Originally intended for ALTER TABLE SPLIT PARTITION operations that were subsequently reverted
- Contains only the minimal NodeTag field required for the PostgreSQL node system
- Should not be used in new code development
- Serves as an example of how PostgreSQL maintains backward compatibility even when functionality is removed
- The stub nature indicates this may be removed in future major versions when ABI compatibility is not required
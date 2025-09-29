# LogicalTapeCreate

## Location
[src/backend/utils/sort/logtape.c:680-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L680-L695)

## Overview
Creates a new logical tape within an existing tape set, initialized in write state, with restrictions preventing tape creation in parallel sort leader processes.

## Definition
```c
LogicalTape *LogicalTapeCreate(LogicalTapeSet *lts)
```

## Detailed Description
The `LogicalTapeCreate` function creates a new LogicalTape within an existing LogicalTapeSet. The function serves as a public interface wrapper around the internal `ltsCreateTape` function, adding important safety checks for parallel processing scenarios.

In parallel sort operations, the function enforces a critical restriction: leader processes cannot create new tapes. This limitation exists because BufFiles opened using shared filesets are read-only in the leader process context. The leader's role is to import and read tapes created by worker processes, not to create new ones. Attempting to create a tape in a leader process will result in an error.

The newly created tape is initialized in write state, ready to accept data. The tape shares the underlying BufFile storage with other tapes in the same set, using logical addressing to maintain separation between different tape contents.

## Parameters / Member Variables
- `lts`: Pointer to the LogicalTapeSet in which to create the new tape

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging and reporting)
  - [ltsCreateTape](../l/ltsCreateTape.md) (internal tape creation function)
  - [LogicalTapeSet](LogicalTapeSet.md) (structure type)
  - [LogicalTape](LogicalTape.md) (structure type)
- Called from (representative examples):
  - [hashagg_spill_init](../h/hashagg_spill_init.md) (hash aggregation spill initialization)
  - [selectnewtape](../s/selectnewtape.md) (tuplesort tape selection)

## Notes and Other Information
- The function includes explicit error checking to prevent tape creation in leader processes during parallel operations
- Leader processes should use `LogicalTapeImport()` instead to claim tapes created by workers
- The restriction against leader tape creation could potentially be lifted if BufFiles were made writable in shared contexts
- Newly created tapes start in write mode and must be rewound or frozen before reading
- The function delegates actual tape creation to the internal `ltsCreateTape` function
- Error message "cannot create new tapes in leader process" indicates parallel processing constraint violation
- The tape creation process allocates logical addressing space within the shared BufFile storage
- Each tape maintains independent state despite sharing underlying file storage with other tapes in the set

## Simplified Source

```c
LogicalTape *LogicalTapeCreate(LogicalTapeSet *lts) {
    // Prevent tape creation in parallel sort leader processes
    // Leaders can only read shared tapes, not create new ones
    if (lts->fileset && lts->worker == -1) {
        elog(ERROR, "cannot create new tapes in leader process");
    }

    // Create and return new tape in write state
    return ltsCreateTape(lts);
}
```
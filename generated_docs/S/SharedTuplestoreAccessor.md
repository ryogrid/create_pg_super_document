# SharedTuplestoreAccessor

## Location
src/backend/utils/sort/sharedtuplestore.c: 71 - 103

## Overview
SharedTuplestoreAccessor is the per-participant local state structure that provides an interface for reading from and writing to shared tuple stores in PostgreSQL parallel query execution.

## Definition
```c
struct SharedTuplestoreAccessor
{
    int         participant;    /* My participant number. */
    SharedTuplestore *sts;      /* The shared state. */
    SharedFileSet *fileset;     /* The SharedFileSet holding files. */
    MemoryContext context;      /* Memory context for buffers. */

    /* State for reading. */
    int         read_participant;       /* The current participant to read from. */
    BufFile    *read_file;             /* The current file to read from. */
    int         read_ntuples_available; /* The number of tuples in chunk. */
    int         read_ntuples;          /* How many tuples have we read from chunk? */
    size_t      read_bytes;            /* How many bytes have we read from chunk? */
    char       *read_buffer;           /* A buffer for loading tuples. */
    size_t      read_buffer_size;
    BlockNumber read_next_page;        /* Lowest block well consider reading. __pycache__/ any-script-mcp-repo/ any-script-mcp/ checkpointing_documentation/ contrib/ data/ generated_docs/ log/ misc/ official_doc_in_md/ output/ prompts/ scripts/ src/ topic_specific_generated_docs/
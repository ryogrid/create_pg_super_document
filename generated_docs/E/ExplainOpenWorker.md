# ExplainOpenWorker

## Location
[src/backend/commands/explain.c:4498-4559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4498-L4559)

## Overview
ExplainOpenWorker is a static function in PostgreSQL's explain module that begins or resumes output redirection to a separate buffer for collecting per-worker statistics during parallel query execution explanation.

## Definition

```c
static void
ExplainOpenWorker(int n, ExplainState *es)
```
## Detailed Description
This function manages the complex task of redirecting explain output to worker-specific buffers during parallel query plan explanation. It handles two scenarios: initializing output for a worker encountered for the first time, and resuming output for a worker that has already produced some data.

For first-time initialization, it creates a new StringInfo buffer, sets up proper formatting state with ExplainOpenSetAsideGroup, and optionally emits a "Worker Number" field for non-text formats. For resuming workers, it restores the previously saved formatting state.

The function also handles format-specific behavior: in TEXT format, it prefixes the first line with "Worker N:" and increases indentation for subsequent lines to create a visually organized hierarchy.

## Parameters / Member Variables
- : The worker number/index (0-based) to open output for
- : ExplainState structure containing formatting options and worker state

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validates worker state and bounds)
  - initStringInfo (initializes worker's string buffer)
  - [ExplainOpenSetAsideGroup](ExplainOpenSetAsideGroup.md) (sets up formatting group for worker)
  - [ExplainPropertyInteger](ExplainPropertyInteger.md) (emits worker number in non-TEXT formats)
  - [ExplainRestoreGroup](ExplainRestoreGroup.md) (restores saved formatting state)
  - [ExplainIndentText](ExplainIndentText.md) (handles text indentation)
  - appendStringInfo (adds worker prefix to TEXT output)
- Called from:
  - [ExplainNode](ExplainNode.md) (various locations when processing parallel execution data)
  - [show_sort_info](../s/show_sort_info.md), show_incremental_sort_info, show_memoize_info, show_hashagg_info (for worker-specific statistics)

## Notes and Other Information
- Saves the previous output buffer pointer to enable proper restoration later
- Allows one extra logical nesting level to accommodate the eventual "Workers" group wrapper
- Handles format-specific rendering: structured data for JSON/XML, prefixed indented text for TEXT format
- Works in tandem with ExplainCloseWorker to manage worker output sessions
- Part of the infrastructure that enables coherent per-worker statistics presentation in EXPLAIN output
- File location: src/backend/commands/explain.c:4498-4559
# pickss

## Location
[src/backend/regex/rege_dfa.c:1044-1102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L1044-L1102)

## Overview
A static function that selects the next state set to be used in the DFA regex engine, implementing a cache replacement strategy that prioritizes reusing older, unlocked state sets.

## Definition

```c
static struct sset *
pickss(struct vars *v,
	   struct dfa *d,
	   chr *cp,
	   chr *start)
```
## Detailed Description
The  function implements a sophisticated state set allocation strategy for PostgreSQL's regex DFA engine. It operates in two phases: first, it attempts to allocate a new state set if the cache isn't full (fast path). If the cache is full, it employs a replacement algorithm that targets state sets that are old enough to be considered expendable, specifically those that haven't been seen in the oldest 33% of the character processing window.

The replacement algorithm uses a circular search pattern starting from  to find victim state sets. It calculates an "ancient" threshold based on the current position and search history, then looks for unlocked state sets that were last seen before this threshold. The function maintains the  pointer to avoid repeatedly checking the same state sets, providing better cache locality and performance.

## Parameters / Member Variables
- `*v`: Pointer to the vars structure containing regex execution variables and context
- `*d`: Pointer to the DFA structure representing the finite automaton
- `*cp`: Current character pointer in the input string being matched
- `*start`: Pointer to the start of the input string being processed
## Dependencies
- Functions called/Symbols referenced:
  - FDEBUG (debugging macro for operation tracing)
  - WHITE (color constant used as default value)
  - LOCKED (flag constant to check if state set is protected)
  - ERR (error reporting macro)
  - REG_ASSERT (regex assertion error constant)
- Called from (representative examples):
  - [getvacant](../g/getvacant.md) (to obtain a candidate state set for reuse)
  - LOFF (in regex execution engine)

## Notes and Other Information
The function implements a careful balance between performance and memory usage. When the cache is not full, it provides O(1) allocation by simply using the next available slot. When replacement is needed, it uses a heuristic that assumes character positions further from the current position are less likely to be needed again. The "ancient" threshold of 33% ensures reasonable cache turnover while avoiding premature eviction of potentially useful state sets. If no suitable victim can be found, the function reports a critical assertion error, indicating a serious problem with the DFA's state management.

## Simplified Source

```c
static struct sset *pickss(struct vars *v, struct dfa *d, chr *cp, chr *start) {
    int i;
    struct sset *ss;
    chr *ancient;

    // Fast path: if cache isn't full, allocate new state set
    if (d->nssused < d->nssets) {
        i = d->nssused++;
        ss = &d->ssets[i];

        // Initialize the new state set
        ss->states = &d->statesarea[i * d->wordsper];
        ss->flags = 0;
        ss->ins.ss = NULL;
        ss->ins.co = WHITE;
        ss->outs = &d->outsarea[i * d->ncolors];
        ss->inchain = &d->incarea[i * d->ncolors];

        // Clear output and chain arrays
        for (i = 0; i < d->ncolors; i++) {
            ss->outs[i] = NULL;
            ss->inchain[i].ss = NULL;
        }
        return ss;
    }

    // Cache is full - find a victim state set to reuse
    // Calculate "ancient" threshold (oldest 33% are expendable)
    if (cp - start > d->nssets * 2 / 3)
        ancient = cp - d->nssets * 2 / 3;
    else
        ancient = start;

    // Search from current search position to end
    for (ss = d->search; ss < &d->ssets[d->nssets]; ss++) {
        if ((ss->lastseen == NULL || ss->lastseen < ancient) &&
            !(ss->flags & LOCKED)) {
            d->search = ss + 1;  // Update search position
            return ss;
        }
    }

    // Search from beginning to current search position
    for (ss = d->ssets; ss < d->search; ss++) {
        if ((ss->lastseen == NULL || ss->lastseen < ancient) &&
            !(ss->flags & LOCKED)) {
            d->search = ss + 1;  // Update search position
            return ss;
        }
    }

    // Critical error: no suitable victim found
    ERR(REG_ASSERT);
    return NULL;
}
```
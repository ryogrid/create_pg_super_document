# report_triggers

## Location
[src/backend/commands/explain.c:1202-1272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1202-L1272)

## Overview
Generates execution statistics report for triggers associated with a single relation, including timing information and call counts when available.

## Definition

```c
static void
report_triggers(ResultRelInfo *rInfo, bool show_relname, ExplainState *es)
```
## Detailed Description
The  function is responsible for reporting execution statistics for all triggers associated with a specific relation during query execution. It iterates through all triggers defined on the relation and outputs performance metrics including execution time and number of calls. The function only reports triggers that were actually invoked during the query execution, ignoring those that were never triggered as they are likely not relevant to the current query type.

The output format depends on the explain format specified in the ExplainState. For text format, it provides a compact representation with optional verbose details, while non-text formats (JSON, XML, YAML) include all available information in a structured manner.

## Parameters / Member Variables
- `*rInfo`: ResultRelInfo structure containing relation information and trigger instrumentation data
- `show_relname`: Boolean flag indicating whether to include the relation name in the output
- `*es`: ExplainState structure containing formatting options and output buffer
## Dependencies
- Functions called/Symbols referenced:
  - [InstrEndLoop](../I/InstrEndLoop.md)
  - [ExplainOpenGroup](../E/ExplainOpenGroup.md)
  - RelationGetRelationName
  - [get_constraint_name](../g/get_constraint_name.md)
  - [ExplainPropertyText](../E/ExplainPropertyText.md)
  - [ExplainPropertyFloat](../E/ExplainPropertyFloat.md)
  - [ExplainCloseGroup](../E/ExplainCloseGroup.md)
- Called from (representative examples):
  - [ExplainPrintTriggers](../E/ExplainPrintTriggers.md)

## Notes and Other Information
- The function cleans up instrumentation state for each trigger using InstrEndLoop
- Triggers with zero tuple counts (never invoked) are skipped in the output
- For constraint triggers, both trigger name and constraint name may be displayed
- Timing information is only included when es->timing is enabled
- Memory allocated for constraint names is properly freed using pfree
- Output formatting varies significantly between text and structured formats

## Simplified Source

```c
static void report_triggers(ResultRelInfo *rInfo, bool show_relname, ExplainState *es) {
    // Return early if no triggers or instrumentation data
    if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
        return;

    // Iterate through all triggers on the relation
    for (int nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++) {
        Trigger *trig = rInfo->ri_TrigDesc->triggers + nt;
        Instrumentation *instr = rInfo->ri_TrigInstrument + nt;

        // Clean up instrumentation state
        InstrEndLoop(instr);

        // Skip triggers that were never invoked
        if (instr->ntuples == 0)
            continue;

        ExplainOpenGroup("Trigger", NULL, true, es);

        // Get relation and constraint names
        char *relname = RelationGetRelationName(rInfo->ri_RelationDesc);
        char *conname = OidIsValid(trig->tgconstraint) ?
                       get_constraint_name(trig->tgconstraint) : NULL;

        // Format output based on explain format
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            // Text format: compact representation
            appendStringInfo(es->str, "Trigger %s", trig->tgname);
            if (conname)
                appendStringInfo(es->str, " for constraint %s", conname);
            if (show_relname)
                appendStringInfo(es->str, " on %s", relname);

            // Add timing and call information
            if (es->timing)
                appendStringInfo(es->str, ": time=%.3f calls=%.0f\n",
                               1000.0 * instr->total, instr->ntuples);
            else
                appendStringInfo(es->str, ": calls=%.0f\n", instr->ntuples);
        } else {
            // Structured format: detailed properties
            ExplainPropertyText("Trigger Name", trig->tgname, es);
            if (conname)
                ExplainPropertyText("Constraint Name", conname, es);
            ExplainPropertyText("Relation", relname, es);
            if (es->timing)
                ExplainPropertyFloat("Time", "ms", 1000.0 * instr->total, 3, es);
            ExplainPropertyFloat("Calls", NULL, instr->ntuples, 0, es);
        }

        // Clean up memory
        if (conname)
            pfree(conname);

        ExplainCloseGroup("Trigger", NULL, true, es);
    }
}
```
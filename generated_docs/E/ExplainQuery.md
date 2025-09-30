# ExplainQuery

## Location
[src/backend/commands/explain.c:183-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L183-L371)

## Overview
ExplainQuery executes an EXPLAIN command by parsing options, rewriting queries, and generating execution plan output in various formats.

## Definition

```c
void
ExplainQuery(ParseState *pstate, ExplainStmt *stmt,
			 ParamListInfo params, DestReceiver *dest)
```
## Detailed Description
ExplainQuery is the main entry point for processing EXPLAIN commands in PostgreSQL. It handles the complete workflow from parsing EXPLAIN options to generating the final output. The function creates an ExplainState object, parses all EXPLAIN options (analyze, verbose, costs, buffers, wal, settings, timing, memory, serialize, format), validates option combinations, rewrites the query using QueryRewrite, and then explains each resulting query plan. The output can be formatted as text, XML, JSON, or YAML depending on the FORMAT option.

The function performs extensive validation of option combinations, such as ensuring that WAL, TIMING, and SERIALIZE options are only used with ANALYZE, and that GENERIC_PLAN cannot be used with ANALYZE. After processing, it outputs the results through tuple output functions that handle multi-line or single-line formatting based on the chosen format.

## Parameters / Member Variables
- : ParseState containing parser context and source text information
- : ExplainStmt containing the query to explain and list of EXPLAIN options
- : ParamListInfo containing parameter values for parameterized queries
- : DestReceiver specifying where to send the explain output results

## Dependencies
- Functions called/Symbols referenced:
  - [NewExplainState](../N/NewExplainState.md)
  - [defGetBoolean](../d/defGetBoolean.md)/defGetString
  - [QueryRewrite](../Q/QueryRewrite.md)
  - [ExplainBeginOutput](ExplainBeginOutput.md)/ExplainEndOutput
  - [ExplainOneQuery](ExplainOneQuery.md)
  - [ExplainResultDesc](ExplainResultDesc.md)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)
  - [do_text_output_multiline](../d/do_text_output_multiline.md)/do_text_output_oneline
  - [end_tup_output](../e/end_tup_output.md)
  - [JumbleQuery](../J/JumbleQuery.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Supports multiple output formats: TEXT, XML, JSON, YAML
- Handles serialization options for plan data (NONE, TEXT, BINARY)  
- Enforces strict validation rules between different EXPLAIN options
- Uses post_parse_analyze_hook for extensibility if available
- Handles INSTEAD NOTHING rules by showing appropriate message
- Creates tuple descriptors for proper output formatting in different destinations

## Simplified Source

```c
void ExplainQuery(ParseState *pstate, ExplainStmt *stmt,
                 ParamListInfo params, DestReceiver *dest)
{
    ExplainState *es = NewExplainState();
    TupOutputState *tstate;
    JumbleState *jstate = NULL;
    Query *query;
    List *rewritten;
    ListCell *lc;
    bool timing_set = false;
    bool summary_set = false;

    // Parse all EXPLAIN options
    foreach(lc, stmt->options)
    {
        DefElem *opt = (DefElem *) lfirst(lc);

        if (strcmp(opt->defname, "analyze") == 0)
            es->analyze = defGetBoolean(opt);
        else if (strcmp(opt->defname, "verbose") == 0)
            es->verbose = defGetBoolean(opt);
        else if (strcmp(opt->defname, "costs") == 0)
            es->costs = defGetBoolean(opt);
        else if (strcmp(opt->defname, "buffers") == 0)
            es->buffers = defGetBoolean(opt);
        else if (strcmp(opt->defname, "wal") == 0)
            es->wal = defGetBoolean(opt);
        else if (strcmp(opt->defname, "timing") == 0)
        {
            timing_set = true;
            es->timing = defGetBoolean(opt);
        }
        else if (strcmp(opt->defname, "summary") == 0)
        {
            summary_set = true;
            es->summary = defGetBoolean(opt);
        }
        else if (strcmp(opt->defname, "format") == 0)
        {
            char *p = defGetString(opt);
            if (strcmp(p, "text") == 0)
                es->format = EXPLAIN_FORMAT_TEXT;
            else if (strcmp(p, "xml") == 0)
                es->format = EXPLAIN_FORMAT_XML;
            else if (strcmp(p, "json") == 0)
                es->format = EXPLAIN_FORMAT_JSON;
            else if (strcmp(p, "yaml") == 0)
                es->format = EXPLAIN_FORMAT_YAML;
            else
                ereport(ERROR, /* unrecognized format */);
        }
        else if (strcmp(opt->defname, "serialize") == 0)
        {
            // Handle serialize option with text/binary/none values
            if (opt->arg)
            {
                char *p = defGetString(opt);
                if (strcmp(p, "off") == 0 || strcmp(p, "none") == 0)
                    es->serialize = EXPLAIN_SERIALIZE_NONE;
                else if (strcmp(p, "text") == 0)
                    es->serialize = EXPLAIN_SERIALIZE_TEXT;
                else if (strcmp(p, "binary") == 0)
                    es->serialize = EXPLAIN_SERIALIZE_BINARY;
                else
                    ereport(ERROR, /* unrecognized serialize option */);
            }
            else
                es->serialize = EXPLAIN_SERIALIZE_TEXT;
        }
        // ... handle other options ...
        else
            ereport(ERROR, /* unrecognized EXPLAIN option */);
    }

    // Validate option combinations
    if (es->wal && !es->analyze)
        ereport(ERROR, /* WAL requires ANALYZE */);

    if (es->timing && !es->analyze)
        ereport(ERROR, /* TIMING requires ANALYZE */);

    if (es->serialize != EXPLAIN_SERIALIZE_NONE && !es->analyze)
        ereport(ERROR, /* SERIALIZE requires ANALYZE */);

    if (es->generic && es->analyze)
        ereport(ERROR, /* ANALYZE and GENERIC_PLAN cannot be used together */);

    // Set defaults for timing and summary if not explicitly set
    es->timing = timing_set ? es->timing : es->analyze;
    es->summary = summary_set ? es->summary : es->analyze;

    // Prepare query for processing
    query = castNode(Query, stmt->query);
    if (IsQueryIdEnabled())
        jstate = JumbleQuery(query);

    if (post_parse_analyze_hook)
        (*post_parse_analyze_hook)(pstate, query, jstate);

    // Rewrite the query
    rewritten = QueryRewrite(castNode(Query, stmt->query));

    // Generate explain output
    ExplainBeginOutput(es);

    if (rewritten == NIL)
    {
        // Handle INSTEAD NOTHING case
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoString(es->str, "Query rewrites to nothing\n");
    }
    else
    {
        // Explain each rewritten query
        foreach(lc, rewritten)
        {
            ExplainOneQuery(lfirst_node(Query, lc), CURSOR_OPT_PARALLEL_OK,
                           NULL, es, pstate->p_sourcetext, params, pstate->p_queryEnv);

            // Add separator between multiple plans
            if (lnext(rewritten, lc) != NULL)
                ExplainSeparatePlans(es);
        }
    }

    ExplainEndOutput(es);

    // Output results to destination
    tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt), &TTSOpsVirtual);
    if (es->format == EXPLAIN_FORMAT_TEXT)
        do_text_output_multiline(tstate, es->str->data);
    else
        do_text_output_oneline(tstate, es->str->data);
    end_tup_output(tstate);

    pfree(es->str->data);
}
```
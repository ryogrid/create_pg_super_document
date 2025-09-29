# fireRules

## Location
[src/backend/rewrite/rewriteHandler.c:2381-2471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L2381-L2471)

## Overview
Iterates through rule locks applying rules, handling both qualified and unqualified INSTEAD rules while generating appropriate rule actions and modified original queries.

## Definition

```c
static List *
fireRules(Query *parsetree,
		  int rt_index,
		  CmdType event,
		  List *locks,
		  bool *instead_flag,
		  bool *returning_flag,
		  Query **qual_product)
```
## Detailed Description
fireRules is a core function in PostgreSQL's rule rewriting system that processes a list of rules and generates the appropriate rewritten queries. The function handles different types of rules with sophisticated logic:

1. **Unqualified INSTEAD rules**: Replace the original query entirely
2. **Qualified INSTEAD rules**: Generate both the rule action (with qualification) and a modified original query (with negated qualification) for the "else" case
3. **Non-INSTEAD rules**: Generate additional actions alongside the original query

For qualified INSTEAD rules, the function creates a modified version of the original query where all the negated rule qualifications are ANDed together, ensuring the original query only executes when none of the INSTEAD rules' conditions are met. This implements a "default case" behavior.

The function also tracks whether any unqualified INSTEAD rules are found (which means the original query should be completely suppressed) and whether any rules modify the RETURNING clause.

## Parameters / Member Variables
- : The original query being rewritten
- : Range table index of the result relation in the original query
- : Type of rule event (INSERT, UPDATE, DELETE)
- : List of RewriteRule structures to process
- : Output parameter set to true if any unqualified INSTEAD rule is found
- : Output parameter set to true if any rule rewrites the RETURNING clause
- : Output parameter filled with modified original query for qualified INSTEAD rules

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - [CopyAndAddInvertedQual](../C/CopyAndAddInvertedQual.md)
  - [rewriteRuleAction](../r/rewriteRuleAction.md)
  - [lappend](../l/lappend.md)
  - [RewriteRule](../R/RewriteRule.md) (struct)
  - QuerySource (enum)
  - CmdType (enum)
  - QSRC_QUAL_INSTEAD_RULE, QSRC_INSTEAD_RULE, QSRC_NON_INSTEAD_RULE (constants)
  - CMD_NOTHING (constant)
- Called from (representative examples):
  - [RewriteQuery](../R/RewriteQuery.md)

## Notes and Other Information
- Central function for processing rule locks and generating rewritten query actions
- Implements complex logic for qualified vs unqualified INSTEAD rules
- Creates "default case" behavior by negating qualified INSTEAD rule conditions
- Sets appropriate QuerySource values to track rule types for later processing
- Handles CMD_NOTHING actions by skipping them entirely
- Part of the PostgreSQL rule system that enables views, triggers, and custom rewriting logic
- The qual_product mechanism allows multiple qualified INSTEAD rules to be combined properly

## Simplified Source

```c
static List *fireRules(Query *parsetree, int rt_index, CmdType event,
                      List *locks, bool *instead_flag,
                      bool *returning_flag, Query **qual_product)
{
    List *results = NIL;
    ListCell *l;

    foreach(l, locks)
    {
        RewriteRule *rule_lock = (RewriteRule *) lfirst(l);
        Node *event_qual = rule_lock->qual;
        List *actions = rule_lock->actions;
        QuerySource qsrc;
        ListCell *r;

        // Determine the query source type based on rule type
        if (rule_lock->isInstead)
        {
            if (event_qual != NULL)
                qsrc = QSRC_QUAL_INSTEAD_RULE;
            else
            {
                qsrc = QSRC_INSTEAD_RULE;
                *instead_flag = true;  // Report unqualified INSTEAD
            }
        }
        else
            qsrc = QSRC_NON_INSTEAD_RULE;

        // For qualified INSTEAD rules, create modified original query
        if (qsrc == QSRC_QUAL_INSTEAD_RULE)
        {
            // Build the "default case" query with negated rule qualifications
            // Only if we haven't found an unqualified INSTEAD rule yet
            if (!*instead_flag)
            {
                if (*qual_product == NULL)
                    *qual_product = copyObject(parsetree);
                *qual_product = CopyAndAddInvertedQual(*qual_product,
                                                      event_qual,
                                                      rt_index,
                                                      event);
            }
        }

        // Process each action in the rule
        foreach(r, actions)
        {
            Query *rule_action = lfirst(r);

            if (rule_action->commandType == CMD_NOTHING)
                continue;

            rule_action = rewriteRuleAction(parsetree, rule_action,
                                          event_qual, rt_index, event,
                                          returning_flag);

            rule_action->querySource = qsrc;
            rule_action->canSetTag = false;  // May change later

            results = lappend(results, rule_action);
        }
    }

    return results;
}
```
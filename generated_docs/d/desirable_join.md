# desirable_join

## Location
src/backend/optimizer/geqo/geqo_eval.c: 325 - 338

## Overview
A heuristic function that determines whether two relations should be joined immediately or postponed during GEQO tree construction.

## Definition


## Detailed Description
The desirable_join function implements the core heuristic logic used by gimme_tree to decide when to join two relations during genetic query optimization. Rather than forcing all possible joins, this function applies intelligent criteria to determine which joins are beneficial to perform immediately versus those that should be postponed.

The function uses two primary criteria for determining join desirability: the presence of relevant join clauses (WHERE conditions that connect the two relations) and explicit join order restrictions that force certain relations to be joined together. This heuristic approach helps GEQO avoid generating poor quality plans that include unnecessary Cartesian products while ensuring that semantically required joins are not overlooked.

By returning true only for joins that have clear logical or semantic justification, the function enables gimme_tree to build higher quality bushy plans that respect the query structure while avoiding inefficient join orders.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning context and join-related metadata
- : RelOptInfo for the first relation being considered for joining
- : RelOptInfo for the second relation being considered for joining

## Dependencies
- Functions called/Symbols referenced:
  - [have_relevant_joinclause](../h/have_relevant_joinclause.md) (checks for WHERE clauses connecting the relations)
  - [have_join_order_restriction](../h/have_join_order_restriction.md) (checks for explicit join ordering constraints)

- Called from (representative examples):
  - [merge_clump](../m/merge_clump.md) (during clump merging decisions in gimme_tree)

## Notes and Other Information
- Implements a conservative approach - only approves joins with clear justification
- Helps prevent Cartesian products by postponing joins without relevant conditions
- Critical for the quality improvement seen in GEQO's bushy plan generation
- Works in conjunction with the force parameter in merge_clump for two-phase joining
- Simple but effective heuristic that significantly impacts query plan quality
- Part of the improved GEQO implementation that can generate bushy plans unlike earlier left-sided-only versions
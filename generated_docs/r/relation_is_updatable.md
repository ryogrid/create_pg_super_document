# relation_is_updatable

## Location
src/backend/rewrite/rewriteHandler.c: 2854 - 2862

## Overview
Determines which update events (INSERT, UPDATE, DELETE) a specified relation supports, considering rules, triggers, and the underlying relation structure.

## Definition
```c
int relation_is_updatable(Oid reloid,
                          List *outer_reloids,
                          bool include_triggers,
                          Bitmapset *include_cols)
```

## Detailed Description
The `relation_is_updatable` function analyzes a PostgreSQL relation to determine what types of data modification operations it supports. This function is central to PostgreSQLs updatable view system and is used by the information_schema views to classify relations as updatable or trigger-updatable.
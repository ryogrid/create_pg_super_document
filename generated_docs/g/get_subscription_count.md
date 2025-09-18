# get_subscription_count

## Location
src/bin/pg_upgrade/info.c: 748 - 762

## Overview
The get_subscription_count function retrieves the total number of logical replication subscriptions in a PostgreSQL cluster.

## Definition


## Detailed Description
This function queries the pg_catalog.pg_subscription system catalog to count all logical replication subscriptions present in the cluster. It connects to the template1 database to execute a simple COUNT(*) query against the pg_subscription table and stores the result in the cluster's nsubs field. This information is used by pg_upgrade to determine whether subscription-related upgrade steps need to be performed. The function provides essential metadata about logical replication configuration that affects the upgrade process, particularly in scenarios where subscriptions need to be handled or validated during cluster migration.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure where the subscription count will be stored in the nsubs field

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - atoi
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md)
  - fopen_priv

## Notes and Other Information
- Uses template1 database for system catalog access like other metadata collection functions
- Stores result directly in cluster->nsubs field for later use by upgrade logic
- Simple implementation with straightforward COUNT(*) query against pg_subscription
- Part of the broader metadata collection phase that occurs early in the upgrade process
- Function has external linkage (not static), making it available to other compilation units
- Essential for determining if logical replication subscriptions exist that may affect upgrade behavior
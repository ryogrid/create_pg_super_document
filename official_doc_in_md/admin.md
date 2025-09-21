Part III. Server Administration  
---  
[Prev](parallel-safety.md "15.4. Parallel Safety") | [Up](index.md "PostgreSQL 17.5 Documentation")| PostgreSQL 17.5 Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](install-binaries.md "Chapter 16. Installation from Binaries")  
  
* * *

# Part III. Server Administration

This part covers topics that are of interest to a PostgreSQL administrator. This includes installation, configuration of the server, management of users and databases, and maintenance tasks. Anyone running PostgreSQL server, even for personal use, but especially in production, should be familiar with these topics. 

The information attempts to be in the order in which a new user should read it. The chapters are self-contained and can be read individually as desired. The information is presented in a narrative form in topical units. Readers looking for a complete description of a command are encouraged to review the [Part VI](reference.md "Part VI. Reference"). 

The first few chapters are written so they can be understood without prerequisite knowledge, so new users who need to set up their own server can begin their exploration. The rest of this part is about tuning and management; that material assumes that the reader is familiar with the general use of the PostgreSQL database system. Readers are encouraged review the [Part I](tutorial.md "Part I. Tutorial") and [Part II](sql.md "Part II. The SQL Language") parts for additional information. 

**Table of Contents**

[16\. Installation from Binaries](install-binaries.md)
[17\. Installation from Source Code](installation.md)
    

[17.1. Requirements](install-requirements.md)
[17.2. Getting the Source](install-getsource.md)
[17.3. Building and Installation with Autoconf and Make](install-make.md)
[17.4. Building and Installation with Meson](install-meson.md)
[17.5. Post-Installation Setup](install-post.md)
[17.6. Supported Platforms](supported-platforms.md)
[17.7. Platform-Specific Notes](installation-platform-notes.md)
[18\. Server Setup and Operation](runtime.md)
    

[18.1. The PostgreSQL User Account](postgres-user.md)
[18.2. Creating a Database Cluster](creating-cluster.md)
[18.3. Starting the Database Server](server-start.md)
[18.4. Managing Kernel Resources](kernel-resources.md)
[18.5. Shutting Down the Server](server-shutdown.md)
[18.6. Upgrading a PostgreSQL Cluster](upgrading.md)
[18.7. Preventing Server Spoofing](preventing-server-spoofing.md)
[18.8. Encryption Options](encryption-options.md)
[18.9. Secure TCP/IP Connections with SSL](ssl-tcp.md)
[18.10. Secure TCP/IP Connections with GSSAPI Encryption](gssapi-enc.md)
[18.11. Secure TCP/IP Connections with SSH Tunnels](ssh-tunnels.md)
[18.12. Registering Event Log on Windows](event-log-registration.md)
[19\. Server Configuration](runtime-config.md)
    

[19.1. Setting Parameters](config-setting.md)
[19.2. File Locations](runtime-config-file-locations.md)
[19.3. Connections and Authentication](runtime-config-connection.md)
[19.4. Resource Consumption](runtime-config-resource.md)
[19.5. Write Ahead Log](runtime-config-wal.md)
[19.6. Replication](runtime-config-replication.md)
[19.7. Query Planning](runtime-config-query.md)
[19.8. Error Reporting and Logging](runtime-config-logging.md)
[19.9. Run-time Statistics](runtime-config-statistics.md)
[19.10. Automatic Vacuuming](runtime-config-autovacuum.md)
[19.11. Client Connection Defaults](runtime-config-client.md)
[19.12. Lock Management](runtime-config-locks.md)
[19.13. Version and Platform Compatibility](runtime-config-compatible.md)
[19.14. Error Handling](runtime-config-error-handling.md)
[19.15. Preset Options](runtime-config-preset.md)
[19.16. Customized Options](runtime-config-custom.md)
[19.17. Developer Options](runtime-config-developer.md)
[19.18. Short Options](runtime-config-short.md)
[20\. Client Authentication](client-authentication.md)
    

[20.1. The `pg_hba.conf` File](auth-pg-hba-conf.md)
[20.2. User Name Maps](auth-username-maps.md)
[20.3. Authentication Methods](auth-methods.md)
[20.4. Trust Authentication](auth-trust.md)
[20.5. Password Authentication](auth-password.md)
[20.6. GSSAPI Authentication](gssapi-auth.md)
[20.7. SSPI Authentication](sspi-auth.md)
[20.8. Ident Authentication](auth-ident.md)
[20.9. Peer Authentication](auth-peer.md)
[20.10. LDAP Authentication](auth-ldap.md)
[20.11. RADIUS Authentication](auth-radius.md)
[20.12. Certificate Authentication](auth-cert.md)
[20.13. PAM Authentication](auth-pam.md)
[20.14. BSD Authentication](auth-bsd.md)
[20.15. Authentication Problems](client-authentication-problems.md)
[21\. Database Roles](user-manag.md)
    

[21.1. Database Roles](database-roles.md)
[21.2. Role Attributes](role-attributes.md)
[21.3. Role Membership](role-membership.md)
[21.4. Dropping Roles](role-removal.md)
[21.5. Predefined Roles](predefined-roles.md)
[21.6. Function Security](perm-functions.md)
[22\. Managing Databases](managing-databases.md)
    

[22.1. Overview](manage-ag-overview.md)
[22.2. Creating a Database](manage-ag-createdb.md)
[22.3. Template Databases](manage-ag-templatedbs.md)
[22.4. Database Configuration](manage-ag-config.md)
[22.5. Destroying a Database](manage-ag-dropdb.md)
[22.6. Tablespaces](manage-ag-tablespaces.md)
[23\. Localization](charset.md)
    

[23.1. Locale Support](locale.md)
[23.2. Collation Support](collation.md)
[23.3. Character Set Support](multibyte.md)
[24\. Routine Database Maintenance Tasks](maintenance.md)
    

[24.1. Routine Vacuuming](routine-vacuuming.md)
[24.2. Routine Reindexing](routine-reindex.md)
[24.3. Log File Maintenance](logfile-maintenance.md)
[25\. Backup and Restore](backup.md)
    

[25.1. SQL Dump](backup-dump.md)
[25.2. File System Level Backup](backup-file.md)
[25.3. Continuous Archiving and Point-in-Time Recovery (PITR)](continuous-archiving.md)
[26\. High Availability, Load Balancing, and Replication](high-availability.md)
    

[26.1. Comparison of Different Solutions](different-replication-solutions.md)
[26.2. Log-Shipping Standby Servers](warm-standby.md)
[26.3. Failover](warm-standby-failover.md)
[26.4. Hot Standby](hot-standby.md)
[27\. Monitoring Database Activity](monitoring.md)
    

[27.1. Standard Unix Tools](monitoring-ps.md)
[27.2. The Cumulative Statistics System](monitoring-stats.md)
[27.3. Viewing Locks](monitoring-locks.md)
[27.4. Progress Reporting](progress-reporting.md)
[27.5. Dynamic Tracing](dynamic-trace.md)
[27.6. Monitoring Disk Usage](diskusage.md)
[28\. Reliability and the Write-Ahead Log](wal.md)
    

[28.1. Reliability](wal-reliability.md)
[28.2. Data Checksums](checksums.md)
[28.3. Write-Ahead Logging (WAL)](wal-intro.md)
[28.4. Asynchronous Commit](wal-async-commit.md)
[28.5. WAL Configuration](wal-configuration.md)
[28.6. WAL Internals](wal-internals.md)
[29\. Logical Replication](logical-replication.md)
    

[29.1. Publication](logical-replication-publication.md)
[29.2. Subscription](logical-replication-subscription.md)
[29.3. Logical Replication Failover](logical-replication-failover.md)
[29.4. Row Filters](logical-replication-row-filter.md)
[29.5. Column Lists](logical-replication-col-lists.md)
[29.6. Conflicts](logical-replication-conflicts.md)
[29.7. Restrictions](logical-replication-restrictions.md)
[29.8. Architecture](logical-replication-architecture.md)
[29.9. Monitoring](logical-replication-monitoring.md)
[29.10. Security](logical-replication-security.md)
[29.11. Configuration Settings](logical-replication-config.md)
[29.12. Quick Setup](logical-replication-quick-setup.md)
[30\. Just-in-Time Compilation (JIT)](jit.md)
    

[30.1. What Is JIT compilation?](jit-reason.md)
[30.2. When to JIT?](jit-decision.md)
[30.3. Configuration](jit-configuration.md)
[30.4. Extensibility](jit-extensibility.md)
[31\. Regression Tests](regress.md)
    

[31.1. Running the Tests](regress-run.md)
[31.2. Test Evaluation](regress-evaluation.md)
[31.3. Variant Comparison Files](regress-variant.md)
[31.4. TAP Tests](regress-tap.md)
[31.5. Test Coverage Examination](regress-coverage.md)

* * *

[Prev](parallel-safety.md "15.4. Parallel Safety") | [Up](index.md "PostgreSQL 17.5 Documentation")|  [Next](install-binaries.md "Chapter 16. Installation from Binaries")  
---|---|---  
15.4. Parallel Safety | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 16. Installation from Binaries

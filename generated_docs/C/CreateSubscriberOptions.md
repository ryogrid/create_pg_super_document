# CreateSubscriberOptions

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:35-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L35-L48)

## Overview
CreateSubscriberOptions is a struct that holds command-line options and configuration parameters for the pg_createsubscriber utility, which converts a PostgreSQL standby server into a logical replication subscriber.

## Definition

```c
struct CreateSubscriberOptions
{
	char	   *config_file;	/* configuration file */
	char	   *pub_conninfo_str;	/* publisher connection string */
	char	   *socket_dir;		/* directory for Unix-domain socket, if any */
	char	   *sub_port;		/* subscriber port number */
	const char *sub_username;	/* subscriber username */
	SimpleStringList database_names;	/* list of database names */
	SimpleStringList pub_names; /* list of publication names */
	SimpleStringList sub_names; /* list of subscription names */
	SimpleStringList replslot_names;	/* list of replication slot names */
	int			recovery_timeout;	/* stop recovery after this time */
};
```
## Detailed Description
CreateSubscriberOptions serves as the central configuration structure for the pg_createsubscriber command-line utility. This struct encapsulates all the necessary parameters required to transform a standby PostgreSQL server into a logical replication subscriber. The structure holds connection information, database and publication specifications, and operational parameters that control the conversion process. It is used throughout the pg_createsubscriber.c module to pass configuration data between functions during the subscriber creation workflow.

## Parameters / Member Variables
- : Path to the PostgreSQL configuration file to be used during the conversion process
- : Connection string used to connect to the publisher database server
- : Directory path for Unix-domain sockets, if Unix sockets are being used for connections
- : Port number on which the subscriber server will listen for connections
- : Username to be used when connecting to the subscriber database
- : List of database names that will be included in the logical replication setup
- : List of publication names on the publisher that will be replicated
- : List of subscription names to be created on the subscriber
- : List of replication slot names to be used for logical replication
- : Timeout value (in seconds) after which the recovery process will be stopped

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleStringList](../S/SimpleStringList.md) (used for storing lists of names)
- Called from (representative examples):
  - [get_sub_conninfo](../g/get_sub_conninfo.md) (src/bin/pg_basebackup/pg_createsubscriber.c:315)
  - store_pub_sub_info (src/bin/pg_basebackup/pg_createsubscriber.c:433)
  - [modify_subscriber_sysid](../m/modify_subscriber_sysid.md) (src/bin/pg_basebackup/pg_createsubscriber.c:629)
  - [start_standby_server](../s/start_standby_server.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1443)
  - [wait_for_end_recovery](../w/wait_for_end_recovery.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1511)
  - [main](../m/main.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1897)

## Notes and Other Information
This structure is specifically designed for the pg_createsubscriber utility and is not used elsewhere in the PostgreSQL codebase. The struct uses SimpleStringList for managing multiple database, publication, subscription, and replication slot names, allowing for flexible configuration of multiple objects in a single operation. The structure is closely related to LogicalRepInfo, which uses CreateSubscriberOptions as a member to store configuration data alongside runtime information during the subscriber creation process.
CREATE DATABASE IF NOT EXISTS sdp CHARACTER SET UTF8;

USE sdp;

CREATE TABLE `feed_in_version` (
    `meta_key` varchar(128) PRIMARY KEY,                 # type, schema, version, e.g. hbase|assetanalysis:rt_cust_pl_mkt_index_info|master
    `type` varchar(64) NOT NULL,                         # e.g. hbase mysql etc
    `meta_name` varchar(128) NOT NULL,                   # e.g. for hbase is namespace:table, for mssql is db..tbl, for mysql is db.tbl
    `write_version` varchar(64) NOT NULL,
    `read_version` varchar(64) NOT NULL,
    `modifier` varchar(64),
    `modify_time` varchar(25),
    `attributes` varchar(1024) default NULL              # json
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `feed_in_time` (
    `meta_key` varchar(128) PRIMARY KEY,                 # type, schema, e.g. hbase|assetanalysis:rt_cust_pl_mkt_index_info
    `feed_in_time` varchar(25),                          # e.g. 2020-01-01 12:00:00
    `last_data_time` varchar(25),                        # last time update data time, might for a specified day, e.g. yesterday, format 20210407
    `latest_data_time` varchar(25),                      # the latest data time, should be today, but init time might be 6/7 am, so before that still be yesterday e.g. 20210407
    `type` varchar(64) NOT NULL,                         # e.g. hbase mysql etc
    `meta_name` varchar(128) NOT NULL,                   # e.g. for hbase is namespace:table, for mssql is db..tbl, for mysql is db.tbl
    `attributes` varchar(1024) default NULL              # json
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `app_info` (
    `bid` varchar(128) PRIMARY KEY,                      # business id e.g. realtime_martrd_
    `type` varchar(64) NOT NULL,                         # spark, flink, etc
    `state` varchar(64) NOT NULL,                        # running, stopped, etc
    `latest_snapshot` varchar(128),                      # e.g. flink's latest savepoint
    `app_id` varchar(128),                               # e.g. YARN APP ID application_1617852022870_0004
    `job_id` varchar(128),                               # e.g. FLINK ID 48a984a4285c6593a98cbf07c4c5ea49
    `url` varchar(128),                                  # e.g. YARN Tracking URL
    `modifier` varchar(64),
    `modify_time` varchar(25),
    `creator` varchar(64),
    `create_time` varchar(25),
    `attributes` varchar(1024) default NULL              # json
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
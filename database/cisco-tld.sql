CREATE TABLE cisco_tld_queue (
    download_date String,
    `date` Date,
    produce_date String,
    rank UInt64,
    tld String
  )
ENGINE = Kafka('redpanda-0:9092', 'cisco-tld-topic', 'umbrella-tld-group1', 'JSONEachRow');
-- ORDER BY (`date`, rank, domain);

CREATE TABLE cisco_tld_data (
    download_date String,
    `date` Date,
    produce_date String,
    rank UInt64,
    tld String
  )
ENGINE ReplacingMergeTree(`date`)
PRIMARY KEY (`date`, rank, tld)
ORDER BY (`date`, rank, tld);

CREATE MATERIALIZED VIEW consumer_tld TO cisco_tld_data
    AS SELECT *
FROM cisco_tld_queue;
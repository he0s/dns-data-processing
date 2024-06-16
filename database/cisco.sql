CREATE TABLE cisco_queue (
    download_date String,
    `date` Date,
    produce_date String,
    rank UInt8,
    domain String
  )
ENGINE = Kafka('redpanda-0:9092', 'cisco-topic', 'umbrella-group1', 'JSONEachRow');
-- ORDER BY (`date`, rank, domain);

CREATE TABLE cisco_data (
    download_date String,
    `date` Date,
    produce_date String,
    rank UInt8,
    domain String
  )
ENGINE ReplacingMergeTree(`date`)
PRIMARY KEY (`date`, rank, domain)
ORDER BY (`date`, rank, domain);

CREATE MATERIALIZED VIEW consumer TO cisco_data
    AS SELECT *
FROM cisco_queue;
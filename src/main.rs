use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::providers::mariadb::gtid::gtid_list::GtidList;
use mysql_cdc::providers::mysql::gtid::gtid_set::GtidSet;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;

use std::collections::BTreeMap;
use std::{thread, time::Duration};
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};
use chrono::{TimeZone, Utc};
use rskafka::client::Client;
use rskafka::client::partition::{OffsetAt, PartitionClient};

struct KafkaProducer {
    client: Client,
    topic: Option<String>,
}

impl KafkaProducer {
    async fn connect(url: String) -> Self {
        KafkaProducer {
            client: ClientBuilder::new(vec![url])
                .build()
                .await
                .expect("Couldn't connect to Kafka"),
            topic: None,
        }
    }

    async fn create_topic(&mut self, topic_name: &str) {
        let topics = self.client.list_topics().await.unwrap();

        for topic in topics {
            if topic.name.eq(&topic_name.to_string()) {
                self.topic = Some(topic_name.to_string());
                println!("Topic already exists in Kafka");
                return;
            }
        }

        let controller_client = self
            .client
            .controller_client()
            .expect("Couldn't create controller client for Kafka");

        controller_client
            .create_topic(
                topic_name,
                1,     // partitions
                1,     // replication factor
                5_000, // timeout (ms)
            )
            .await
            .unwrap();

        self.topic = Some(topic_name.to_string());
    }

    fn create_record(&self, headers: String, value: String) -> Record {
        Record {
            key: None,
            value: Some(value.into_bytes()),
            headers: BTreeMap::from([
                ("mysql_binlog_headers".to_owned(), headers.into_bytes()),
            ]),
            timestamp: Utc.timestamp_millis(42),
        }
    }

    async fn get_partition_client(&self, partition: i32) -> Option<PartitionClient> {
        if self.topic.is_none() {
            return None;
        }

        let topic = self.topic.as_ref().unwrap();
        Some(
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .expect("Couldn't fetch partition client"),
        )
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), mysql_cdc::errors::Error> {
    let sleep_time: u64 = std::env::var("SLEEP_TIME").unwrap().parse().unwrap();

    thread::sleep(Duration::from_millis(sleep_time));
    println!("Thread started");

    let table_names: Vec<&str> = vec!["table1", "table2", "table3"]; // Add the table names

    let options = BinlogOptions::from_end();

    let username = std::env::var("SQL_USERNAME").unwrap();
    let password = std::env::var("SQL_PASSWORD").unwrap();
    let mysql_port = std::env::var("SQL_PORT").unwrap();
    let mysql_hostname = std::env::var("SQL_HOSTNAME").unwrap();
    let mysql_database = std::env::var("SQL_DATABASE").unwrap();

    let options = ReplicaOptions {
        username,
        password,
        port: mysql_port.parse::<u16>().unwrap(),
        hostname: mysql_hostname,
        database: Some(mysql_database),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);
    println!("Connected to MySQL database");

    let kafka_url = std::env::var("KAFKA_URL").unwrap();
    let mut kafka_producer = KafkaProducer::connect(kafka_url).await;
    println!("Connected to Kafka server");

    for result in client.replicate()? {
        let (header, event) = result?;

        let table_name = match event {
            BinlogEvent::TableMapEvent(ref table_event) => {
                let table_id = table_event.table_id();
                let table_schema = table_event.table_schema().to_owned();
                let table_name = table_event.table_name().to_owned();

                format!("{}.{}", table_schema, table_name)
            }
            _ => continue,
        };

        if !table_names.contains(&&table_name[..]) {
            // Skip processing if table name not in the list
            continue;
        }

        let topic_name = format!("{}_{}", mysql_database, table_name);
        kafka_producer.create_topic(&topic_name).await;

        let partition_client = kafka_producer
            .get_partition_client(0)
            .await
            .unwrap();
        let mut partition_offset = partition_client
            .get_offset(OffsetAt::Latest)
            .await
            .unwrap();

        let json_event =
            serde_json::to_string(&event).expect("Couldn't convert SQL event to JSON");
        let json_header =
            serde_json::to_string(&header).expect("Couldn't convert SQL header to JSON");

        let kafka_record = kafka_producer.create_record(json_header, json_event);
        partition_client
            .produce(vec![kafka_record], Compression::default())
            .await
            .unwrap();

        let (records, high_watermark) = partition_client
            .fetch_records(partition_offset, 1..100_000, 1_000)
            .await
            .unwrap();

        partition_offset = high_watermark;

        for record in records {
            let record_clone = record.clone();
            let timestamp = record_clone.record.timestamp;
            let value = record_clone.record.value.unwrap();
            let header = record_clone
                .record
                .headers
                .get("mysql_binlog_headers")
                .unwrap()
                .clone();

            println!(
                "============================================== Event from Apache Kafka ==========================================================================\n\
                 Value: {}\n\
                 Timestamp: {}\n\
                 Headers: {}\n\
                 \n",
                String::from_utf8_lossy(&value),
                timestamp,
                String::from_utf8_lossy(&header)
            );
        }

        client.commit(&header, &event);
    }

    Ok(())
}
# wasmedge-mysql-binlog-kafka
WasmEdge project: A stream log processing framework for WasmEdge! 

To modify the code to meet the requirements:

# 1.)  Add a vector to hold the table names:

let table_names: Vec<&str> = vec!["table1", "table2", "table3"];  // Replace with actual table names


# 2.)  Update the loop where the binlog events are processed to filter events based on the table names:

for result in client.replicate()? {
    let (header, event) = result?;
    
    // Get the table name from the event
    let table_name = match &event {
        BinlogEvent::TableMapEvent(table_map_event) => {
            table_map_event.table_name.clone()
        }
        _ => continue, // Skip events that are not TableMapEvent
    };

    // Check if the table name is in the list of allowed tables
    if !table_names.contains(&table_name.as_str()) {
        continue; // Skip events for tables not in the list
    }
    

# 3.)  Modify the "create_topic" function to create topics with names based on the database and table names:

async fn create_topic(&mut self, database_name: &str, table_name: &str) {
    let topic_name = format!("{}_{}", database_name, table_name);

    let topics = self.client.list_topics().await.unwrap();

    for topic in topics {
        if topic.name.eq(&topic_name) {
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
            &topic_name,
            1,     // partitions
             1,     // replication factor
            5_000, // timeout (ms)
        )
        .await
        .unwrap();

    self.topic = Some(topic_name.to_string());
}


# 4.)  Update the code in the "main" function to pass the database name and table name to the "create_topic" function:

let database_name = std::env::var("SQL_DATABASE").unwrap();

for result in client.replicate()? {
    let (header, event) = result?;

    // Get the table name from the event
    let table_name = match &event {
        BinlogEvent::TableMapEvent(table_map_event) => {
            table_map_event.table_name.clone()
        }
        _ => continue, // Skip events that are not TableMapEvent
    };

    // Check if the table name is in the list of allowed tables
    if !table_names.contains(&table_name.as_str()) {
        continue; // Skip events for tables not in the list
    }

  

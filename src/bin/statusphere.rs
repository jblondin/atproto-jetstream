use std::{
    convert::Infallible,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use atproto_jetstream::{
    connection::{Connection, Cursor, Options, bluesky_instances::US_EAST_1},
    consumer::{Consumer, FlattenedCommitEvent, start_processor},
    multi_consumer,
};
use serde::Deserialize;

#[tokio::main]
async fn main() {
    // needed for tungstenite
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let connection = Connection::new(
        Options::new(US_EAST_1)
            .wanted_collections(["xyz.statusphere.status".to_owned()])
            .compress(true),
    );

    let status_consumer = StatusConsumer;
    let multi_consumer = multi_consumer!(
        StatusMultiConsumer<Infallible> {
            "xyz.statusphere.status" => Status => StatusConsumer = status_consumer
        }
    );

    let two_hours_ago = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time error")
        - Duration::from_secs(2 * 60 * 60);
    let cursor = Cursor::from(two_hours_ago.as_micros() as u64);
    start_processor(multi_consumer, connection, cursor).await;
}

#[derive(Debug, Deserialize)]
struct Status {
    #[serde(rename = "createdAt")]
    created_at: String,
    status: String,
}

#[derive(Debug)]
struct StatusConsumer;

impl Consumer<Status, Infallible> for StatusConsumer {
    async fn consume(&self, message: FlattenedCommitEvent<Status>) -> Result<(), Infallible> {
        println!(
            "Status ({created_at} / {time_us}): {status} by {did}",
            created_at = message.record.created_at,
            time_us = message.time_us,
            status = message.record.status,
            did = message.did
        );
        Ok(())
    }
}

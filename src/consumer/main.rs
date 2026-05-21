use iggy::prelude::*;
use std::env;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let root_username =
        env::var("IGGY_ROOT_USERNAME").unwrap_or_else(|_| DEFAULT_ROOT_USERNAME.to_string());
    let root_password =
        env::var("IGGY_ROOT_PASSWORD").map_err(|_| "IGGY_ROOT_PASSWORD must be set (see .env)")?;
    let stream_name =
        env::var("IGGY_STREAM_NAME").map_err(|_| "IGGY_STREAM_NAME must be set (see .env)")?;
    let topic_name =
        env::var("IGGY_TOPIC_NAME").map_err(|_| "IGGY_TOPIC_NAME must be set (see .env)")?;
    let partition_id = env::var("IGGY_PARTITION_ID")
        .map_err(|_| "IGGY_PARTITION_ID must be set (see .env)")?
        .parse::<u32>()
        .map_err(|_| "IGGY_PARTITION_ID must be a valid u32")?;

    let client = IggyClient::default();
    client.connect().await?;
    client.login_user(&root_username, &root_password).await?;
    consume_messages(&client, &stream_name, &topic_name, partition_id).await
}

async fn consume_messages(client: &IggyClient, stream_name: &str, topic_name: &str, partition_id: u32) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {} ms.",
        stream_name,
        topic_name,
        partition_id,
        interval.as_millis()
    );

    let mut offset = 0;
    let messages_per_batch = 10;
    let consumer = Consumer::default();
    loop {
        let polled_messages = client
            .poll_messages(
                &stream_name.try_into()?,
                &topic_name.try_into()?,
                Some(partition_id),
                &consumer,
                &PollingStrategy::offset(offset),
                messages_per_batch,
                false,
            )
            .await?;

        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            sleep(interval).await;
            continue;
        }

        offset += polled_messages.messages.len() as u64;
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
        sleep(interval).await;
    }
}

fn handle_message(message: &IggyMessage) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.header.offset, payload
    );
    Ok(())
}

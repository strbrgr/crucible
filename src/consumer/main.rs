use iggy::prelude::*;
use std::env;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

struct Config {
    root_username: String,
    root_password: String,
    stream_name: String,
    topic_name: String,
    partition_id: u32,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            root_username: env::var("IGGY_ROOT_USERNAME")
                .unwrap_or_else(|_| DEFAULT_ROOT_USERNAME.to_string()),
            root_password: env::var("IGGY_ROOT_PASSWORD")
                .map_err(|_| "IGGY_ROOT_PASSWORD must be set (see .env)")?,
            stream_name: env::var("IGGY_STREAM_NAME")
                .map_err(|_| "IGGY_STREAM_NAME must be set (see .env)")?,
            topic_name: env::var("IGGY_TOPIC_NAME")
                .map_err(|_| "IGGY_TOPIC_NAME must be set (see .env)")?,
            partition_id: env::var("IGGY_PARTITION_ID")
                .map_err(|_| "IGGY_PARTITION_ID must be set (see .env)")?
                .parse::<u32>()
                .map_err(|_| "IGGY_PARTITION_ID must be a valid u32")?,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let config = Config::from_env()?;
    let client = IggyClient::default();
    client.connect().await?;
    client
        .login_user(&config.root_username, &config.root_password)
        .await?;
    consume_messages(&client, &config).await
}

async fn consume_messages(client: &IggyClient, config: &Config) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {} ms.",
        config.stream_name,
        config.topic_name,
        config.partition_id,
        interval.as_millis()
    );

    let mut offset = 0;
    let messages_per_batch = 10;
    let consumer = Consumer::default();
    loop {
        let polled_messages = client
            .poll_messages(
                &config.stream_name.as_str().try_into()?,
                &config.topic_name.as_str().try_into()?,
                Some(config.partition_id),
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

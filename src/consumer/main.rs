use iggy::prelude::*;
use questdb::ingress::{Buffer, Sender, TimestampNanos};
use sensor_scenario::SensorReading;
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
    qdb_client_conf: String,
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
            qdb_client_conf: env::var("QDB_CLIENT_CONF")
                .map_err(|_| "QDB_CLIENT_CONF must be set (see .env)")?,
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

    let stream_id = Identifier::try_from(config.stream_name.as_str())?;
    let topic_id = Identifier::try_from(config.topic_name.as_str())?;
    let mut offset = 0;
    let messages_per_batch = 10;
    let consumer = Consumer::default();
    let mut sender = Sender::from_conf(config.qdb_client_conf.as_str())?;

    loop {
        let polled_messages = client
            .poll_messages(
                &stream_id,
                &topic_id,
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
            handle_message(&message, &mut sender)?;
        }
        sleep(interval).await;
    }
}

fn handle_message(message: &IggyMessage, sender: &mut Sender) -> Result<(), Box<dyn Error>> {
    let sensor_reading = serde_json::from_slice::<SensorReading>(&message.payload)?;
    let mut buffer = Buffer::new(questdb::ingress::ProtocolVersion::V3);

    match sensor_reading {
        SensorReading::Temperature(reading) => {
            buffer
                .table("temperature")?
                .symbol("unit", reading.unit)?
                .column_i64("value", reading.value.into())?
                .at(TimestampNanos::new(reading.created_at))?;
        }
        SensorReading::Humidity(reading) => {
            buffer
                .table("humidity")?
                .column_f64("value", reading.value.into())?
                .at(TimestampNanos::new(reading.created_at))?;
        }
    }

    sender.flush(&mut buffer)?;

    Ok(())
}

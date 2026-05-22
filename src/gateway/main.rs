use std::{env, error::Error, str::FromStr, sync::Arc};

use iggy::prelude::{
    Client, CompressionAlgorithm, DEFAULT_ROOT_USERNAME, Identifier, IggyClient, IggyDuration,
    IggyExpiry, IggyMessage, MaxTopicSize, MessageClient, Partitioning, StreamClient, TopicClient,
    UserClient,
};
use sensor_scenario::SensorReading;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};
use tracing::{error, info, warn};

struct Config {
    root_username: String,
    root_password: String,
    stream_name: Arc<str>,
    topic_name: Arc<str>,
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
                .map_err(|_| "IGGY_STREAM_NAME must be set (see .env)")?
                .into(),
            topic_name: env::var("IGGY_TOPIC_NAME")
                .map_err(|_| "IGGY_TOPIC_NAME must be set (see .env)")?
                .into(),
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

    let config = Arc::new(Config::from_env()?);
    let client = Arc::new(IggyClient::default());
    client.connect().await?;
    client
        .login_user(&config.root_username, &config.root_password)
        .await?;

    let (stream_id, topic_id) =
        init_system(&client, &config.stream_name, &config.topic_name).await?;
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let client = Arc::clone(&client);
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            let _ = handle_client(stream, client, stream_id, topic_id, config).await;
        });
    }
}

async fn init_system(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
) -> Result<(u32, u32), Box<dyn Error>> {
    let stream_ident = Identifier::named(stream_name)?;
    let topic_ident = Identifier::named(topic_name)?;

    let stream = match client.create_stream(stream_name).await {
        Ok(stream) => {
            info!("Stream was created.");
            stream
        }
        Err(_) => {
            warn!("Stream already exists and will not be created again.");
            client
                .get_stream(&stream_ident)
                .await?
                .ok_or("stream not found after create-already-exists")?
        }
    };

    let topic = match client
        .create_topic(
            &stream_ident,
            topic_name,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
    {
        Ok(topic) => {
            info!("Topic was created.");
            topic
        }
        Err(_) => {
            warn!("Topic already exists and will not be created again.");
            client
                .get_topic(&stream_ident, &topic_ident)
                .await?
                .ok_or("topic not found after create-already-exists")?
        }
    };

    Ok((stream.id, topic.id))
}

async fn handle_client(
    mut stream: TcpStream,
    client: Arc<IggyClient>,
    stream_id: u32,
    topic_id: u32,
    config: Arc<Config>,
) -> Result<(), Box<dyn Error>> {
    let duration = IggyDuration::from_str("500ms")?;
    info!(
        "Messages will be sent to stream: {} ({}), topic: {} ({}), partition: {} with interval {}.",
        config.stream_name,
        stream_id,
        config.topic_name,
        topic_id,
        config.partition_id,
        duration.as_human_time_string()
    );

    let messages_per_batch = 10;
    let partitioning = Partitioning::partition_id(config.partition_id);
    let mut messages = Vec::new();

    loop {
        let mut incoming_message_len_buf = [0u8; 4];

        if stream
            .read_exact(&mut incoming_message_len_buf)
            .await
            .is_err()
        {
            return Ok(());
        }

        let incoming_message_len = u32::from_be_bytes(incoming_message_len_buf) as usize;

        let mut buf = vec![0u8; incoming_message_len];
        stream.read_exact(&mut buf).await?;

        // Validation gate
        let unvalidated_message = serde_json::from_slice::<SensorReading>(&buf);
        if unvalidated_message.is_err() {
            error!("Message is corrupted.")
            // TODO: publish to DLQ or different topic
        }
        let message = IggyMessage::from(String::from_utf8(buf)?);
        messages.push(message);

        if messages.len() == messages_per_batch {
            client
                .send_messages(
                    &Identifier::named(&config.stream_name)?,
                    &Identifier::named(&config.topic_name)?,
                    &partitioning,
                    &mut messages,
                )
                .await?;

            info!("Sent {messages_per_batch} message(s).");
            messages.clear();
        }
    }
}

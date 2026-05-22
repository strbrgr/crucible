use uuid::Uuid;

use crate::reading::{SensorType, generate_sensor_reading};
use std::{
    env, io::Write, net::TcpStream, process::exit, str::FromStr, thread::sleep, time::Duration,
};

pub mod reading;

struct Config {
    sensor_type: SensorType,
    frequency: u8,
    tcp_stream: TcpStream,
    sensor_id: Uuid,
}

impl Config {
    pub fn build(args: &[String]) -> Result<Config, &'static str> {
        match args.len() {
            3 => {
                let sensor_type = SensorType::from_str(&args[1])?;

                let frequency = args[2]
                    .parse::<u8>()
                    .map_err(|_| "<frequency> needs to be between 0-255.")?;

                let tcp_stream =
                    TcpStream::connect("127.0.0.1:8080").map_err(|_| "Error connecting via Tcp")?;

                let sensor_id = Uuid::new_v4();

                Ok(Config {
                    sensor_type,
                    frequency,
                    tcp_stream,
                    sensor_id,
                })
            }
            _ => Err("Usage: <sensor type> <frequency>"),
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    let mut config = Config::build(&args).unwrap_or_else(|err| {
        println!("{err}");
        exit(1);
    });

    run(&mut config)?;

    Ok(())
}

fn run(config: &mut Config) -> std::io::Result<()> {
    loop {
        let reading = generate_sensor_reading(&config.sensor_type, config.sensor_id);
        let json = serde_json::to_vec(&reading)?;
        let len = json.len() as u32;

        // Send length first
        config.tcp_stream.write_all(&len.to_be_bytes())?;
        // send actual content
        config.tcp_stream.write_all(&json)?;

        let duration = Duration::new(config.frequency as u64, 0);
        sleep(duration);
    }
}

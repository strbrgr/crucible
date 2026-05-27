use rand::random_range;
use sensor_scenario::{HumidityReading, SensorReading, TemperatureReading, TemperatureUnit};
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub enum SensorType {
    Temperature,
    Humidity,
}

impl FromStr for SensorType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "temperature" => Ok(SensorType::Temperature),
            "humidity" => Ok(SensorType::Humidity),
            _ => Err("Passed in <sensor_type> is not an option."),
        }
    }
}

pub fn generate_sensor_reading(sensor_type: &SensorType, sensor_id: Uuid) -> SensorReading {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    match sensor_type {
        SensorType::Temperature => {
            let reading = TemperatureReading {
                id: sensor_id,
                value: random_range(10..=42),
                unit: TemperatureUnit::Celsius,
                created_at: ts,
            };
            SensorReading::Temperature(reading)
        }

        SensorType::Humidity => {
            let reading = HumidityReading {
                id: Uuid::new_v4(),
                value: (random_range(0.0_f32..=99.99_f32) * 10.0).round() / 10.0,
                created_at: ts,
            };
            SensorReading::Humidity(reading)
        }
    }
}

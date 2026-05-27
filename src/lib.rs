use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
}

impl AsRef<str> for TemperatureUnit {
    fn as_ref(&self) -> &str {
        match self {
            TemperatureUnit::Celsius => "c",
            TemperatureUnit::Fahrenheit => "f",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TemperatureReading {
    pub id: Uuid,
    pub value: u8,
    pub unit: TemperatureUnit,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HumidityReading {
    pub id: Uuid,
    pub value: f32,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SensorReading {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
}

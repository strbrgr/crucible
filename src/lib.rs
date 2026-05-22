use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TemperatureReading {
    pub id: Uuid,
    pub value: u8,
    pub unit: TemperatureUnit,
    pub created_at: u128,
}

#[derive(Serialize, Deserialize)]
pub struct HumidityReading {
    pub id: Uuid,
    pub value: f32,
    pub created_at: u128,
}

#[derive(Serialize, Deserialize)]
pub enum SensorReading {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
}

// Cargo.toml dependencies:
// [dependencies]
// reqwest = { version = "0.11", features = ["stream"] }
// tokio = { version = "1", features = ["full"] }
// futures = "0.3"
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// chrono = "0.4"

use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio::time::{sleep, Duration};

#[derive(Debug, Deserialize, Serialize)]
struct PriceUpdate {
    binary: BinaryData,
    parsed: Vec<ParsedPrice>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BinaryData {
    encoding: String,
    data: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ParsedPrice {
    id: String,
    price: PriceData,
    ema_price: PriceData,
    metadata: Option<Metadata>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PriceData {
    price: String,
    conf: String,
    expo: i32,
    publish_time: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Metadata {
    slot: u64,
    proof_available_time: i64,
    prev_publish_time: i64,
}

async fn connect_and_stream(price_feed_id: &str) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let url = format!(
        "https://hermes.pyth.network/v2/updates/price/stream?ids[]={}",
        price_feed_id
    );

    println!("🔌 Connecting to Pyth Network SSE stream...");
    println!("📡 URL: {}", url);
    println!("🎯 Price Feed ID: {} (BTC/USD)\n", price_feed_id);

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(format!("❌ Failed to connect: {}", response.status()).into());
    }

    println!("✅ Connected successfully!");
    println!("📊 Streaming live price updates...\n");
    println!("═══════════════════════════════════════════════════════════════════════════\n");

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut update_count = 0;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                buffer.push_str(&text);

                // Process complete SSE messages (separated by \n\n)
                while let Some(pos) = buffer.find("\n\n") {
                    let message = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();

                    update_count += 1;
                    process_sse_message(&message, update_count);
                }
            }
            Err(e) => {
                eprintln!("❌ Error reading stream: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn process_sse_message(message: &str, count: u32) {
    for line in message.lines() {
        if line.starts_with("data:") {
            let data = &line[5..].trim(); // Skip "data:" prefix and trim whitespace
            
            match serde_json::from_str::<PriceUpdate>(data) {
                Ok(update) => {
                    for price_data in &update.parsed {
                        display_price(price_data, count);
                    }
                    
                    // Display binary data info
                    println!("    📦 Binary Encoding: {}", update.binary.encoding);
                    println!("    📏 Binary Data Length: {} bytes", 
                        update.binary.data.get(0).map(|s| s.len() / 2).unwrap_or(0));
                }
                Err(e) => {
                    eprintln!("⚠️  Failed to parse price update: {}", e);
                    eprintln!("📄 Raw data: {}", &data[..data.len().min(200)]);
                }
            }
        }
    }
}

fn display_price(price_data: &ParsedPrice, count: u32) {
    // Parse price values
    let price_value = price_data.price.price.parse::<f64>().unwrap_or(0.0);
    let expo = price_data.price.expo;
    let actual_price = price_value * 10f64.powi(expo);
    
    let conf_value = price_data.price.conf.parse::<f64>().unwrap_or(0.0);
    let actual_conf = conf_value * 10f64.powi(expo);
    
    // Parse EMA price
    let ema_price_value = price_data.ema_price.price.parse::<f64>().unwrap_or(0.0);
    let ema_actual_price = ema_price_value * 10f64.powi(price_data.ema_price.expo);
    
    // Format timestamp
    let timestamp = price_data.price.publish_time;
    let datetime = chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| timestamp.to_string());

    println!("{:?}", actual_price);
    println!("{:?}", actual_conf);
    println!("{:?}", ema_actual_price);

}

async fn stream_with_reconnect(price_feed_id: &str) {
    let mut attempt = 1;
    
    loop {
        println!("\n🔄 Connection attempt #{}", attempt);
        
        match connect_and_stream(price_feed_id).await {
            Ok(_) => {
                println!("ℹ️  Stream ended normally (24 hour limit reached)");
            }
            Err(e) => {
                eprintln!("❌ Stream error: {}", e);
            }
        }

        println!("⏳ Reconnecting in 5 seconds...\n");
        sleep(Duration::from_secs(5)).await;
        attempt += 1;
    }
}

#[tokio::main]
async fn main() {
    // BTC/USD price feed ID
    let btc_usd_feed = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43";

    stream_with_reconnect(btc_usd_feed).await;
}
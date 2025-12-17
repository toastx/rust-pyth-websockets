use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

// ============================================================================
// DATA STRUCTURES
// ============================================================================

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PriceFeed {
    id: String,
    attributes: FeedAttributes,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FeedAttributes {
    symbol: String,
    asset_type: String,
    #[serde(default)]
    base: Option<String>,
    #[serde(default)]
    quote_currency: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PriceUpdate {
    binary: BinaryData,
    parsed: Vec<ParsedPrice>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BinaryData {
    encoding: String,
    data: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ParsedPrice {
    id: String,
    price: PriceData,
    ema_price: PriceData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PriceData {
    price: String,
    conf: String,
    expo: i32,
    publish_time: i64,
}

// Application state to store all feeds and live prices
struct AppState {
    // Feed registry (all available feeds)
    feed_registry: HashMap<String, PriceFeed>,
    symbol_to_id: HashMap<String, String>,

    // Tier 1: Top 100 tokens (static, always streaming)
    top_100_feeds: Vec<String>,

    // Tier 2: Aggregated user watchlists
    user_watchlists: HashMap<UserId, HashSet<String>>, // user -> feed_ids
    watched_feeds: HashSet<String>,                    // All unique feeds being watched

    // Tier 3: Additional feeds (memecoins, etc.)
    additional_feeds: HashMap<String, PriceFeed>,

    // Live price data (shared)
    live_prices: Arc<RwLock<HashMap<String, ParsedPrice>>>,

    // Active streaming feeds
    currently_streaming: HashSet<String>,
    stream_handle: Option<JoinHandle<()>>,
}

type UserId = String;

impl AppState {
    async fn start_streaming(&mut self) {
        let feeds_to_stream = self.get_active_feeds();
        self.currently_streaming = feeds_to_stream.iter().cloned().collect();

        println!("📡 Starting stream with {} feeds", feeds_to_stream.len());
        println!("   - Top 100: {}", self.top_100_feeds.len());
        println!("   - User watched: {}", self.watched_feeds.len());

        let state = self.live_prices.clone();

        let handle = tokio::spawn(async move {
            loop {
                match stream_multiple_feeds(feeds_to_stream.clone(), state.clone()).await {
                    Ok(_) => {
                        println!("Stream ended normally");
                    }
                    Err(e) => {
                        eprintln!("❌ Stream error: {}", e);
                    }
                }
                println!("⏳ Reconnecting in 5 seconds...");
                sleep(Duration::from_secs(5)).await;
            }
        });

        self.stream_handle = Some(handle);
    }

    async fn restart_stream(&mut self) {
        println!("🔄 Restarting stream...");

        // Abort old stream
        if let Some(handle) = self.stream_handle.take() {
            handle.abort();
        }

        // Small delay to ensure clean shutdown
        sleep(Duration::from_millis(100)).await;

        // Start new stream
        self.start_streaming().await;
    }

    // Calculate which feeds need to be streamed
    fn get_active_feeds(&self) -> Vec<String> {
        let mut active = HashSet::new();

        // Always include top 100
        active.extend(self.top_100_feeds.iter().cloned());

        // Add all watched feeds (that aren't already in top 100)
        active.extend(self.watched_feeds.iter().cloned());

        active.into_iter().collect()
    }

    // Add token to user's watchlist
    async fn add_to_watchlist(&mut self, user_id: UserId, symbol: &str) {
        if let Some(feed_id) = self.symbol_to_id.get(symbol) {
            // Add to user's personal watchlist
            self.user_watchlists
                .entry(user_id)
                .or_insert_with(HashSet::new)
                .insert(feed_id.clone());

            // Add to global watched feeds
            let was_new = self.watched_feeds.insert(feed_id.clone());

            // Only restart stream if this feed wasn't already being watched
            if was_new && !self.top_100_feeds.contains(feed_id) {
                println!("➕ New feed added: {}", symbol);
                self.restart_stream().await;
            } else {
                println!("✓ Feed already streaming: {}", symbol);
            }
        }
    }

    // Remove token from user's watchlist
    async fn remove_from_watchlist(&mut self, user_id: &UserId, symbol: &str) {
        if let Some(feed_id) = self.symbol_to_id.get(symbol) {
            // Remove from user's watchlist
            if let Some(watchlist) = self.user_watchlists.get_mut(user_id) {
                watchlist.remove(feed_id);
            }

            // Check if ANY other user is still watching this feed
            let still_watched = self
                .user_watchlists
                .values()
                .any(|watchlist| watchlist.contains(feed_id));

            if !still_watched {
                // No one watching anymore, remove from stream
                self.watched_feeds.remove(feed_id);

                // Only restart if not in top 100
                if !self.top_100_feeds.contains(feed_id) {
                    println!("➖ Feed removed: {}", symbol);
                    self.restart_stream().await;
                }
            }
        }
    }

    // Get user's watchlist prices
    async fn get_user_watchlist(&self, user_id: &UserId) -> Vec<WatchlistItem> {
        let prices = self.live_prices.read().await;

        if let Some(watchlist) = self.user_watchlists.get(user_id) {
            watchlist
                .iter()
                .filter_map(|feed_id| {
                    let price = prices.get(feed_id)?;
                    let feed = self.feed_registry.get(feed_id)?;
                    Some(WatchlistItem {
                        symbol: feed.attributes.symbol.clone(),
                        feed_id: feed_id.clone(),
                        price: parse_price(&price.price),
                        confidence: parse_conf(&price.price),
                        timestamp: price.price.publish_time,
                    })
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    // Get top 100 prices
    async fn get_top_100_prices(&self) -> Vec<DisplayItem> {
        let prices = self.live_prices.read().await;

        self.top_100_feeds
            .iter()
            .filter_map(|feed_id| {
                let price = prices.get(feed_id)?;
                let feed = self.feed_registry.get(feed_id)?;
                Some(DisplayItem {
                    symbol: feed.attributes.symbol.clone(),
                    price: parse_price(&price.price),
                    change_24h: None, // Calculate if needed
                })
            })
            .collect()
    }
}

#[derive(Debug)]
struct WatchlistItem {
    symbol: String,
    feed_id: String,
    price: f64,
    confidence: f64,
    timestamp: i64,
}

#[derive(Debug)]
struct DisplayItem {
    symbol: String,
    price: f64,
    change_24h: Option<f64>,
}

fn parse_price(price_data: &PriceData) -> f64 {
    let value = price_data.price.parse::<f64>().unwrap_or(0.0);
    value * 10f64.powi(price_data.expo)
}

fn parse_conf(price_data: &PriceData) -> f64 {
    let value = price_data.conf.parse::<f64>().unwrap_or(0.0);
    value * 10f64.powi(price_data.expo)
}

// ============================================================================
// FETCH ALL AVAILABLE PRICE FEEDS
// ============================================================================

async fn fetch_all_price_feeds(
    asset_type: Option<&str>,
    query: Option<&str>,
) -> Result<Vec<PriceFeed>, Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let mut url = "https://hermes.pyth.network/v2/price_feeds?".to_string();

    if let Some(at) = asset_type {
        url.push_str(&format!("asset_type={}&", at));
    }
    if let Some(q) = query {
        url.push_str(&format!("query={}", q));
    }

    println!("🔍 Fetching price feeds from: {}", url);

    let response = client.get(&url).send().await?;
    println!("response = {:?}", response);

    if !response.status().is_success() {
        return Err(format!("Failed to fetch feeds: {}", response.status()).into());
    }

    let feeds: Vec<PriceFeed> = response.json().await?;

    println!("✅ Loaded {} price feeds into state\n", feeds.len());

    Ok(feeds)
}

fn select_top_100(feeds: &[PriceFeed]) -> Vec<String> {
    // For now, just take first 100 crypto feeds
    // In production, you'd sort by market cap or use a predefined list
    feeds
        .iter()
        .filter(|f| f.attributes.asset_type == "Crypto")
        .take(100)
        .map(|f| f.id.clone())
        .collect()
}

// ============================================================================
// STREAM PRICES FOR MULTIPLE FEEDS
// ============================================================================

async fn stream_multiple_feeds(
    feed_ids: Vec<String>,
    state: Arc<RwLock<HashMap<String, ParsedPrice>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = Client::new();

    println!("feed_ids = {:#?}", feed_ids);
    // Build URL with multiple feed IDs
    let mut url = "https://hermes.pyth.network/v2/updates/price/stream?".to_string();
    for (i, id) in feed_ids.iter().enumerate() {
        url.push_str(&format!("ids[]=0x{}", id));
        if i < feed_ids.len() - 1 {
            url.push_str("&");
        }
    }

    println!("📡 Connecting to stream with {} feeds...", feed_ids.len());
    println!("🔗 {}\n", url);

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(format!("Failed to connect: {}", response.status()).into());
    }

    println!("✅ Connected! Streaming prices...\n");

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                buffer.push_str(&text);

                // Process complete SSE messages
                while let Some(pos) = buffer.find("\n\n") {
                    let message = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();

                    process_sse_message(&message, state.clone()).await;
                }
            }
            Err(e) => {
                eprintln!("❌ Stream error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn process_sse_message(message: &str, state: Arc<RwLock<HashMap<String, ParsedPrice>>>) {
    for line in message.lines() {
        if line.starts_with("data:") {
            let data = line[5..].trim();

            match serde_json::from_str::<PriceUpdate>(data) {
                Ok(update) => {
                    let mut prices = state.write().await;

                    for price_data in update.parsed {
                        // Store in state
                        prices.insert(price_data.id.clone(), price_data.clone());
                    }
                }
                Err(e) => {
                    eprintln!("⚠️  Parse error: {}", e);
                }
            }
        }
    }
}

// ============================================================================
// DISPLAY CURRENT PRICES FROM STATE
// ============================================================================

async fn display_live_prices(
    state: Arc<RwLock<HashMap<String, ParsedPrice>>>,
    feeds: &[PriceFeed],
) {
    loop {
        sleep(Duration::from_secs(2)).await;

        let prices = state.read().await;

        if prices.is_empty() {
            continue;
        }

        // Clear screen (optional)
        print!("\x1B[2J\x1B[1;1H");

        println!("╔═══════════════════════════════════════════════════════════════════════════╗");
        println!("║                       LIVE PRICE DASHBOARD                                ║");
        println!(
            "║                  {} feeds tracked in state                              ║",
            prices.len()
        );
        println!("╠═══════════════════════════════════════════════════════════════════════════╣\n");

        for (feed_id, price_data) in prices.iter() {
            // Find the feed info
            if let Some(feed) = feeds.iter().find(|f| f.id == *feed_id) {
                let price_value = price_data.price.price.parse::<f64>().unwrap_or(0.0);
                let expo = price_data.price.expo;
                let actual_price = price_value * 10f64.powi(expo);

                let conf_value = price_data.price.conf.parse::<f64>().unwrap_or(0.0);
                let actual_conf = conf_value * 10f64.powi(expo);
                println!(
                    "│ {} ({})                                                    │",
                    feed.attributes.symbol, feed.attributes.asset_type
                );
                println!(
                    "│  💰 Price:      ${:>15.2}                                      │",
                    actual_price
                );
                println!(
                    "│  📊 Confidence: ±${:>14.2}                                      │",
                    actual_conf
                );
                println!(
                    "│  🕐 Updated:    {} seconds ago                                        │",
                    chrono::Utc::now().timestamp() - price_data.price.publish_time
                );
            }
        }
    }
}

// ============================================================================
// SELECT FEEDS INTERACTIVELY
// ============================================================================

fn select_feeds_by_type(feeds: &[PriceFeed], asset_type: &str, limit: usize) -> Vec<String> {
    feeds
        .iter()
        .filter(|f| f.attributes.asset_type == asset_type)
        .take(limit)
        .map(|f| f.id.clone())
        .collect()
}

fn select_feeds_by_symbols(feeds: &[PriceFeed], symbols: &[&str]) -> Vec<String> {
    feeds
        .iter()
        .filter(|f| {
            symbols.iter().any(|s| {
                f.attributes
                    .symbol
                    .to_lowercase()
                    .contains(&s.to_lowercase())
            })
        })
        .map(|f| f.id.clone())
        .collect()
}

async fn initialize_app_state() -> Result<AppState, Box<dyn Error + Send + Sync>> {
    println!("🚀 Initializing Pyth Price Service...\n");

    // 1. Fetch all available feeds
    println!("📥 Loading all available feeds...");
    let all_feeds = fetch_all_price_feeds(Some("crypto"), None).await?;
    println!("✅ Loaded {} crypto feeds\n", all_feeds.len());

    // 2. Build registries
    let mut feed_registry = HashMap::new();
    let mut symbol_to_id = HashMap::new();

    for feed in &all_feeds {
        feed_registry.insert(feed.id.clone(), feed.clone());
        symbol_to_id.insert(feed.attributes.symbol.clone(), feed.id.clone());
    }

    // 3. Select top 100 by market cap (or predefined list)
    println!("🔝 Selecting top 100 tokens...");
    let top_100_feeds = select_top_100(&all_feeds);
    println!(
        "✅ Top 100 selected: {}\n",
        top_100_feeds
            .iter()
            .filter_map(|id| feed_registry.get(id))
            .map(|f| f.attributes.symbol.as_str())
            .take(10)
            .collect::<Vec<_>>()
            .join(", ")
    );

    let state = AppState {
        feed_registry,
        symbol_to_id,
        top_100_feeds,
        user_watchlists: HashMap::new(),
        watched_feeds: HashSet::new(),
        additional_feeds: HashMap::new(),
        live_prices: Arc::new(RwLock::new(HashMap::new())),
        currently_streaming: HashSet::new(),
        stream_handle: None,
    };

    Ok(state)
}

// ============================================================================
// MAIN APPLICATION
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize
    let mut state = initialize_app_state().await?;

    // Start streaming top 100
    state.start_streaming().await;

    // Simulate user interactions
    let user1 = "user_123".to_string();
    let user2 = "user_456".to_string();

    sleep(Duration::from_secs(5)).await;

    // User 1 adds PEPE to watchlist (not in top 100)
    println!("\n👤 User 1 adds PEPE to watchlist");
    state.add_to_watchlist(user1.clone(), "PEPE/USD").await;

    sleep(Duration::from_secs(5)).await;

    // User 2 also adds PEPE (no restart needed, already streaming)
    println!("\n👤 User 2 adds PEPE to watchlist");
    state.add_to_watchlist(user2.clone(), "PEPE/USD").await;

    sleep(Duration::from_secs(5)).await;

    // User 1 removes PEPE (still streaming for User 2)
    println!("\n👤 User 1 removes PEPE from watchlist");
    state.remove_from_watchlist(&user1, "PEPE/USD").await;

    sleep(Duration::from_secs(5)).await;

    // User 2 removes PEPE (now no one watching, remove from stream)
    println!("\n👤 User 2 removes PEPE from watchlist");
    state.remove_from_watchlist(&user2, "PEPE/USD").await;

    // Keep running
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

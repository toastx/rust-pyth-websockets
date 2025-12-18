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
    feed_registry: HashMap<String, PriceFeed>,
    symbol_to_id: HashMap<String, String>,
    top_100_feeds: Vec<String>,
    user_watchlists: HashMap<UserId, HashSet<String>>,
    watched_feeds: HashSet<String>,
    additional_feeds: HashMap<String, PriceFeed>,
    live_prices: Arc<RwLock<HashMap<String, ParsedPrice>>>,
    currently_streaming: HashSet<String>,
    stream_handle: Option<JoinHandle<()>>,
}

type UserId = String;

impl AppState {
    fn start_streaming(&mut self) {
        let feeds_to_stream = self.get_active_feeds();
        self.currently_streaming = feeds_to_stream.iter().cloned().collect();

        println!("📡 Starting stream with {} feeds", feeds_to_stream.len());
        println!("   - Top 100: {}", self.top_100_feeds.len());
        println!("   - User watched: {}", self.watched_feeds.len());

        let state = self.live_prices.clone();

        // Spawn non-blocking task
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

    fn restart_stream(&mut self) {
        println!("🔄 Restarting stream...");

        // Abort old stream
        if let Some(handle) = self.stream_handle.take() {
            handle.abort();
        }

        // Start new stream (non-blocking)
        self.start_streaming();
    }

    fn get_active_feeds(&self) -> Vec<String> {
        let mut active = HashSet::new();
        active.extend(self.top_100_feeds.iter().cloned());
        active.extend(self.watched_feeds.iter().cloned());
        active.into_iter().collect()
    }

    fn add_to_watchlist(&mut self, user_id: UserId, symbol: &str) {
        println!("🔍 Looking up symbol: {}", symbol);
        
        if let Some(feed_id) = self.symbol_to_id.get(symbol) {
            println!("   Found feed_id: {}", feed_id);
            
            self.user_watchlists
                .entry(user_id.clone())
                .or_insert_with(HashSet::new)
                .insert(feed_id.clone());

            let was_new = self.watched_feeds.insert(feed_id.clone());
            
            println!("   User {} watchlist now has {} items", user_id, 
                self.user_watchlists.get(&user_id).map(|s| s.len()).unwrap_or(0));
            println!("   Global watched_feeds now has {} items", self.watched_feeds.len());

            if was_new && !self.top_100_feeds.contains(feed_id) {
                println!("➕ New feed added: {} (not in top 100, restarting stream)", symbol);
                self.restart_stream();
            } else if self.top_100_feeds.contains(feed_id) {
                println!("✓ Feed already in top 100: {} (no restart needed)", symbol);
            } else {
                println!("✓ Feed already being watched by someone else: {}", symbol);
            }
        } else {
            println!("⚠️  Symbol not found in registry: {}", symbol);
            println!("   Available symbols (first 10): {:?}", 
                self.symbol_to_id.keys().take(10).collect::<Vec<_>>());
        }
    }

    fn remove_from_watchlist(&mut self, user_id: &UserId, symbol: &str) {
        if let Some(feed_id) = self.symbol_to_id.get(symbol) {
            if let Some(watchlist) = self.user_watchlists.get_mut(user_id) {
                watchlist.remove(feed_id);
            }

            let still_watched = self
                .user_watchlists
                .values()
                .any(|watchlist| watchlist.contains(feed_id));

            if !still_watched {
                self.watched_feeds.remove(feed_id);

                if !self.top_100_feeds.contains(feed_id) {
                    println!("➖ Feed removed: {}", symbol);
                    self.restart_stream();
                }
            }
        }
    }

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
}

#[derive(Debug)]
struct WatchlistItem {
    symbol: String,
    feed_id: String,
    price: f64,
    confidence: f64,
    timestamp: i64,
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

    if !response.status().is_success() {
        return Err(format!("Failed to fetch feeds: {}", response.status()))?;
    }

    let feeds: Vec<PriceFeed> = response.json().await?;

    println!("✅ Loaded {} price feeds into state\n", feeds.len());

    Ok(feeds)
}

fn select_top_100(feeds: &[PriceFeed]) -> Vec<String> {
    // Priority list of major tokens to include
    let priority_symbols = [
        "BTC/USD", "ETH/USD", "SOL/USD", "USDT/USD", "USDC/USD",
        "BNB/USD", "XRP/USD", "ADA/USD", "DOGE/USD", "MATIC/USD",
        "DOT/USD", "SHIB/USD", "AVAX/USD", "LINK/USD", "UNI/USD",
        "ATOM/USD", "LTC/USD", "APT/USD", "ARB/USD", "OP/USD",
        "PEPE/USD", "WIF/USD", "BONK/USD", "FLOKI/USD",
    ];
    
    let mut selected = Vec::new();
    
    // First, add priority symbols
    for symbol in priority_symbols {
        for feed in feeds {
            if feed.attributes.symbol == format!("Crypto.{}", symbol) {
                selected.push(feed.id.clone());
                break;
            }
        }
    }
    
    // Then fill remaining slots with other crypto feeds
    for feed in feeds {
        if feed.attributes.asset_type == "Crypto" && !selected.contains(&feed.id) {
            selected.push(feed.id.clone());
            if selected.len() >= 100 {
                break;
            }
        }
    }
    
    selected
}

// ============================================================================
// STREAM PRICES FOR MULTIPLE FEEDS
// ============================================================================

async fn stream_multiple_feeds(
    feed_ids: Vec<String>,
    state: Arc<RwLock<HashMap<String, ParsedPrice>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = Client::new();

    let mut url = "https://hermes.pyth.network/v2/updates/price/stream?".to_string();
    for (i, id) in feed_ids.iter().enumerate() {
        url.push_str(&format!("ids[]=0x{}", id));
        if i < feed_ids.len() - 1 {
            url.push_str("&");
        }
    }

    println!("📡 Connecting to stream with {} feeds...", feed_ids.len());

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(format!("Failed to connect: {}", response.status()))?;
    }

    println!("✅ Connected! Streaming prices...\n");

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                buffer.push_str(&text);

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

async fn display_watchlist_only(state: Arc<RwLock<AppState>>) {
    loop {
        sleep(Duration::from_secs(2)).await;

        let state_guard = state.read().await;
        let prices = state_guard.live_prices.read().await;

        print!("\x1B[2J\x1B[1;1H");
        println!("╔════════════════ WATCHLIST ════════════════╗");
        println!("│ Watched feeds: {} | Prices in cache: {}   ", 
            state_guard.watched_feeds.len(), 
            prices.len()
        );
        println!("├───────────────────────────────────────────┤");

        if state_guard.watched_feeds.is_empty() {
            println!("│ (empty - no symbols added yet)           │");
        } else {
            for feed_id in &state_guard.watched_feeds {
                let feed = match state_guard.feed_registry.get(feed_id) {
                    Some(f) => f,
                    None => {
                        println!("│ [UNKNOWN] feed_id: {}...", &feed_id[..8]);
                        continue;
                    }
                };

                match prices.get(feed_id) {
                    Some(price) => {
                        let raw = price.price.price.parse::<f64>().unwrap_or(0.0);
                        let value = raw * 10f64.powi(price.price.expo);
                        println!("│ {:<10} ${:>12.4}                │", feed.attributes.symbol, value);
                    }
                    None => {
                        println!("│ {:<10} [Waiting for price...]        │", feed.attributes.symbol);
                    }
                }
            }
        }

        println!("╚═══════════════════════════════════════════╝");
        
        // Debug info
        if !state_guard.watched_feeds.is_empty() {
            println!("\n🔍 Debug Info:");
            println!("   Watched feed IDs:");
            for feed_id in &state_guard.watched_feeds {
                if let Some(feed) = state_guard.feed_registry.get(feed_id) {
                    println!("   - {} ({})", feed.attributes.symbol, &feed_id[..16]);
                }
            }
            println!("   Price cache has {} entries", prices.len());
        }
    }
}

async fn initialize_app_state() -> Result<AppState, Box<dyn Error + Send + Sync>> {
    println!("🚀 Initializing Pyth Price Service...\n");

    println!("📥 Loading all available feeds...");
    let all_feeds = fetch_all_price_feeds(Some("crypto"), None).await?;
    println!("✅ Loaded {} crypto feeds\n", all_feeds.len());

    let mut feed_registry = HashMap::new();
    let mut symbol_to_id = HashMap::new();

    for feed in &all_feeds {
        feed_registry.insert(feed.id.clone(), feed.clone());
        
        // Store with full symbol
        symbol_to_id.insert(feed.attributes.symbol.clone(), feed.id.clone());
        
        // Also store without "Crypto." prefix for easier lookup
        if let Some(stripped) = feed.attributes.symbol.strip_prefix("Crypto.") {
            symbol_to_id.insert(stripped.to_string(), feed.id.clone());
        }
    }

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
    let state = Arc::new(RwLock::new(initialize_app_state().await?));
    
    // Start streaming (non-blocking)
    {
        let mut s = state.write().await;
        s.start_streaming();
    } // Lock released here
    
    // Start display task
    let watch_state = state.clone();
    tokio::spawn(async move {
        display_watchlist_only(watch_state).await;
    });

    let user1 = "user_123".to_string();
    let user2 = "user_456".to_string();
    
    sleep(Duration::from_secs(3)).await;
    
    println!("\n👤 User 1 adds PEPE");
    {
        let mut s = state.write().await;
        s.add_to_watchlist(user1.clone(), "PEPE/USD");
    }
    
    sleep(Duration::from_secs(3)).await;
    
    println!("\n👤 User 2 adds PEPE");
    {
        let mut s = state.write().await;
        s.add_to_watchlist(user2.clone(), "PEPE/USD");
    }
    
    sleep(Duration::from_secs(3)).await;
    
    println!("\n👤 User 1 adds BTC");
    {
        let mut s = state.write().await;
        s.add_to_watchlist(user1.clone(), "BTC/USD");
    }
    
    sleep(Duration::from_secs(3)).await;
    
    println!("\n👤 User 2 adds ETH");
    {
        let mut s = state.write().await;
        s.add_to_watchlist(user2.clone(), "ETH/USD");
    }
    
    sleep(Duration::from_secs(5)).await;
    
    println!("\n👤 User 1 removes PEPE");
    {
        let mut s = state.write().await;
        s.remove_from_watchlist(&user1, "PEPE/USD");
    }
    
    sleep(Duration::from_secs(3)).await;
    
    println!("\n👤 User 2 removes PEPE");
    {
        let mut s = state.write().await;
        s.remove_from_watchlist(&user2, "PEPE/USD");
    }

    loop {
        sleep(Duration::from_secs(60)).await;
    }
}
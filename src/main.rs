use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures::{sink::SinkExt, stream::StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
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

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
enum ClientMessage {
    Subscribe { symbol: String },
    Unsubscribe { symbol: String },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMessage {
    PriceUpdate {
        symbol: String,
        price: f64,
        confidence: f64,
        timestamp: i64,
    },
    Error {
        message: String,
    },
    Info {
        message: String,
    },
}

type UserId = String;

// Application state
struct AppState {
    feed_registry: HashMap<String, PriceFeed>,
    symbol_to_id: HashMap<String, String>,
    top_100_feeds: Vec<String>,

    // User Management
    user_watchlists: HashMap<UserId, HashSet<String>>,
    clients: HashMap<UserId, mpsc::UnboundedSender<Message>>,

    // Global Watchlist (Union of all user watchlists + Top 100)
    watched_feeds: HashSet<String>,

    // Data Store
    live_prices: HashMap<String, ParsedPrice>,

    // Stream Management
    currently_streaming: HashSet<String>,
    stream_handle: Option<JoinHandle<()>>,
}

impl AppState {
    fn start_streaming(state_arc: Arc<RwLock<AppState>>) {
        tokio::spawn(async move {
            let feeds_to_stream = {
                let state = state_arc.read().await;
                state.get_active_feeds()
            };

            {
                let mut state = state_arc.write().await;
                state.currently_streaming = feeds_to_stream.iter().cloned().collect();

                println!("📡 Starting stream with {} feeds", feeds_to_stream.len());
                println!("   - Top 100: {}", state.top_100_feeds.len());
                println!("   - User watched: {}", state.watched_feeds.len());
            }

            // Spawn the actual stream loop
            let stream_state = state_arc.clone();
            let handle = tokio::spawn(async move {
                loop {
                    // We need to re-read the active feeds in case they changed before restart
                    let current_feeds = {
                        let s = stream_state.read().await;
                        s.get_active_feeds()
                    };

                    match stream_multiple_feeds(current_feeds, stream_state.clone()).await {
                        Ok(_) => println!("Stream ended normally"),
                        Err(e) => eprintln!("❌ Stream error: {}", e),
                    }
                    println!("⏳ Reconnecting in 5 seconds...");
                    sleep(Duration::from_secs(5)).await;
                }
            });

            let mut state = state_arc.write().await;
            state.stream_handle = Some(handle);
        });
    }

    fn restart_stream(state_arc: Arc<RwLock<AppState>>) {
        tokio::spawn(async move {
            let mut state = state_arc.write().await;
            println!("🔄 Restarting stream...");

            if let Some(handle) = state.stream_handle.take() {
                handle.abort();
            }
            // Release lock before calling start_streaming to avoid deadlock potential
            // (though start_streaming is async/spawned, it's safer)
            drop(state);

            AppState::start_streaming(state_arc.clone());
        });
    }

    fn get_active_feeds(&self) -> Vec<String> {
        let mut active = HashSet::new();
        active.extend(self.top_100_feeds.iter().cloned());
        active.extend(self.watched_feeds.iter().cloned());
        active.into_iter().collect()
    }

    fn add_client(&mut self, user_id: UserId, sender: mpsc::UnboundedSender<Message>) {
        self.clients.insert(user_id, sender);
    }

    fn remove_client(&mut self, user_id: &UserId) {
        self.clients.remove(user_id);
        if let Some(watchlist) = self.user_watchlists.remove(user_id) {
            // Cleanup global watched_feeds
            for feed_id in watchlist {
                self.check_and_remove_global_watch(&feed_id);
            }
        }
        println!("❌ Client disconnected: {}", user_id);
    }

    fn check_and_remove_global_watch(&mut self, feed_id: &str) {
        let still_watched = self.user_watchlists.values().any(|w| w.contains(feed_id));
        if !still_watched {
            self.watched_feeds.remove(feed_id);
        }
    }

    fn add_subscription(&mut self, user_id: &UserId, symbol: &str) -> Option<String> {
        // Strip "Crypto." if user provides it, or ensure we match how we stored it
        let lookup_symbol = symbol.trim_start_matches("Crypto.");

        if let Some(feed_id) = self.symbol_to_id.get(lookup_symbol).cloned() {
            self.user_watchlists
                .entry(user_id.clone())
                .or_default()
                .insert(feed_id.clone());

            let was_new = self.watched_feeds.insert(feed_id.clone());

            if was_new && !self.top_100_feeds.contains(&feed_id) {
                return Some(feed_id); // Return ID to signal restart needed
            }
        }
        None
    }

    fn remove_subscription(&mut self, user_id: &UserId, symbol: &str) -> Option<String> {
        let lookup_symbol = symbol.trim_start_matches("Crypto.");

        if let Some(feed_id) = self.symbol_to_id.get(lookup_symbol).cloned() {
            if let Some(watchlist) = self.user_watchlists.get_mut(user_id) {
                watchlist.remove(&feed_id);
            }

            let still_watched = self.user_watchlists.values().any(|w| w.contains(&feed_id));
            if !still_watched {
                self.watched_feeds.remove(&feed_id);
                if !self.top_100_feeds.contains(&feed_id) {
                    return Some(feed_id); // Signal restart needed
                }
            }
        }
        None
    }
}

// ============================================================================
// WEBSOCKET HANDLER
// ============================================================================

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<RwLock<AppState>>>,
) -> impl IntoResponse {
    let user_id = params
        .get("user_id")
        .cloned()
        .unwrap_or_else(|| format!("anon_{}", uuid::Uuid::new_v4()));

    println!("✨ New connection request: {}", user_id);
    ws.on_upgrade(move |socket| handle_socket(socket, state, user_id))
}

async fn handle_socket(socket: WebSocket, state: Arc<RwLock<AppState>>, user_id: String) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Register client
    {
        let mut s = state.write().await;
        s.add_client(user_id.clone(), tx);
    }

    // Task to send messages from channel to websocket
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Task to receive messages from websocket
    let state_clone = state.clone();
    let uid = user_id.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(action) => {
                        let mut restart_needed = false;
                        let mut response_msg = None;

                        {
                            let mut s = state_clone.write().await;
                            match action {
                                ClientMessage::Subscribe { symbol } => {
                                    println!("📥 {} subscribes to {}", uid, symbol);
                                    if s.add_subscription(&uid, &symbol).is_some() {
                                        restart_needed = true;
                                    }
                                    response_msg = Some(format!("Subscribed to {}", symbol));
                                }
                                ClientMessage::Unsubscribe { symbol } => {
                                    println!("📥 {} unsubscribes from {}", uid, symbol);
                                    if s.remove_subscription(&uid, &symbol).is_some() {
                                        restart_needed = true;
                                    }
                                    response_msg = Some(format!("Unsubscribed from {}", symbol));
                                }
                            }
                        } // Release lock

                        if let Some(msg) = response_msg {
                            // Optional: send ack
                            // send_info(&state_clone, &uid, &msg).await;
                        }

                        if restart_needed {
                            AppState::restart_stream(state_clone.clone());
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse message from {}: {}", uid, e);
                    }
                }
            }
        }
    });

    // Wait for either task to finish (disconnect)
    tokio::select! {
        _ = (&mut send_task) => {},
        _ = (&mut recv_task) => {},
    };

    // Cleanup
    {
        let mut s = state.write().await;
        s.remove_client(&user_id);
    }

    // Check if we need to restart stream (if they were the last watcher of something)
    // Note: We'd need to track what they removed specifically, but remove_client handles global cleanup.
    // Optimization: check if active feeds changed vs currently streaming.
    // For now, we'll let the next add/remove trigger it or lazily update.
}

// ============================================================================
// STREAM LOGIC
// ============================================================================

async fn stream_multiple_feeds(
    feed_ids: Vec<String>,
    state_arc: Arc<RwLock<AppState>>,
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
                    process_sse_message(&message, &state_arc).await;
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

async fn process_sse_message(message: &str, state_arc: &Arc<RwLock<AppState>>) {
    for line in message.lines() {
        if line.starts_with("data:") {
            let data = line[5..].trim();
            if let Ok(update) = serde_json::from_str::<PriceUpdate>(data) {
                // We lock for WRITING to update prices and broadcasting
                // Note: In high freq, we might want to split this, but for now it ensures consistency
                let mut state = state_arc.write().await;

                for price_data in update.parsed {
                    state
                        .live_prices
                        .insert(price_data.id.clone(), price_data.clone());

                    // Broadcast to subscribers
                    broadcast_update(&mut state, &price_data).await;
                }
            }
        }
    }
}

async fn broadcast_update(state: &mut AppState, price: &ParsedPrice) {
    // 1. Find the symbol for this feed ID
    // We need to reverse lookup or store it in price.
    // Luckily price feed registry has it.

    // Optimization: Create a FeedID -> [UserId] map in AppState to avoid iterating all users.
    // For now, iterating 100 users is fine.

    // Find users watching this feed_id
    let mut users_to_notify = Vec::new();

    for (user_id, watchlist) in &state.user_watchlists {
        if watchlist.contains(&price.id) {
            users_to_notify.push(user_id.clone());
        }
    }

    if users_to_notify.is_empty() {
        return;
    }

    // Prepare message
    let feed = state.feed_registry.get(&price.id).unwrap(); // Should exist
    let val = price.price.price.parse::<f64>().unwrap_or(0.0) * 10f64.powi(price.price.expo);
    let conf = price.price.conf.parse::<f64>().unwrap_or(0.0) * 10f64.powi(price.price.expo);

    let msg = ServerMessage::PriceUpdate {
        symbol: feed.attributes.symbol.clone(),
        price: val,
        confidence: conf,
        timestamp: price.price.publish_time,
    };

    let json = serde_json::to_string(&msg).unwrap();

    // Send
    for uid in users_to_notify {
        if let Some(tx) = state.clients.get_mut(&uid) {
            let _ = tx.send(Message::Text(json.clone().into()));
        }
    }
}

// ============================================================================
// INITIALIZATION & MAIN
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
    let feeds: Vec<PriceFeed> = response.json().await?;
    println!("✅ Loaded {} price feeds into state\n", feeds.len());
    Ok(feeds)
}

fn select_top_100(feeds: &[PriceFeed]) -> Vec<String> {
    let priority_symbols = [
        "BTC/USD",
        "ETH/USD",
        "SOL/USD",
        "USDT/USD",
        "USDC/USD",
        "BNB/USD",
        "XRP/USD",
        "ADA/USD",
        "DOGE/USD",
        "MATIC/USD",
        "DOT/USD",
        "SHIB/USD",
        "AVAX/USD",
        "LINK/USD",
        "UNI/USD",
        "ATOM/USD",
        "LTC/USD",
        "APT/USD",
        "ARB/USD",
        "OP/USD",
        "PEPE/USD",
        "WIF/USD",
        "BONK/USD",
        "FLOKI/USD",
    ];
    let mut selected = Vec::new();
    for symbol in priority_symbols {
        for feed in feeds {
            if feed.attributes.symbol == format!("Crypto.{}", symbol) {
                selected.push(feed.id.clone());
                break;
            }
        }
    }
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

async fn initialize_app_state() -> Result<AppState, Box<dyn Error + Send + Sync>> {
    println!("🚀 Initializing Pyth Price Service...\n");
    let all_feeds = fetch_all_price_feeds(Some("crypto"), None).await?;

    let mut feed_registry = HashMap::new();
    let mut symbol_to_id = HashMap::new();

    for feed in &all_feeds {
        feed_registry.insert(feed.id.clone(), feed.clone());
        symbol_to_id.insert(feed.attributes.symbol.clone(), feed.id.clone());
        if let Some(stripped) = feed.attributes.symbol.strip_prefix("Crypto.") {
            symbol_to_id.insert(stripped.to_string(), feed.id.clone());
        }
    }

    let top_100_feeds = select_top_100(&all_feeds);

    Ok(AppState {
        feed_registry,
        symbol_to_id,
        top_100_feeds,
        user_watchlists: HashMap::new(),
        clients: HashMap::new(),
        watched_feeds: HashSet::new(),
        live_prices: HashMap::new(),
        currently_streaming: HashSet::new(),
        stream_handle: None,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 1. Initialize State
    let state = Arc::new(RwLock::new(initialize_app_state().await?));

    // 2. Start initial stream
    AppState::start_streaming(state.clone());

    // 3. Setup Router
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    // 4. Run Server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("🚀 WebSocket Server running on ws://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

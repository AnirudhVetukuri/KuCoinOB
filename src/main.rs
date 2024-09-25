use tokio;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use reqwest;
use std::time::{SystemTime, UNIX_EPOCH};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use url::Url;

// Represents a single order in the order book
#[derive(Debug, Clone)]
struct Order {
    price: f64,
    size: f64,
}

// Represents the order book for ETH-USDT futures
#[derive(Debug)]
pub struct OrderBook {
    bids: Vec<Order>,
    asks: Vec<Order>,
}

impl OrderBook {
    // Instantiate empty order book
    pub fn new() -> Self {
        OrderBook {
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    // Update orders (bids or asks)
    pub fn update(&mut self, data: &Value) {
        if let Some(change) = data.get("change") {
            let change_str = change.as_str().unwrap_or_default();

            if let Some((price, direction, quantity)) = parse_market_data(&change_str) {
                let orders = match direction.as_str() {
                    "buy" => &mut self.bids,
                    "sell" => &mut self.asks,
                    _ => {
                        return;
                    }
                };

                if quantity > 0.0 {
                    if let Some(index) = orders.iter().position(|order| order.price == price) {
                        orders[index].size = quantity;
                    } else {
                        orders.push(Order { price, size: quantity });
                    }
                } else if let Some(index) = orders.iter().position(|order| order.price == price) {
                    orders.remove(index);
                }

                orders.sort_by(|a, b| {
                    if direction == "buy" {
                        b.price.partial_cmp(&a.price).unwrap()
                    } else {
                        a.price.partial_cmp(&b.price).unwrap()
                    }
                });
                orders.truncate(5);
            }
        }
    }

    // Print current state of OrderBook in columnar format
    pub fn print_state(&self) {
        println!("{:<15} {:<15} | {:<15} {:<15}", "Bid Size", "Bid Price", "Ask Price", "Ask Size");
        println!("{}", "-".repeat(60));

        for i in 0..5 {
            let bid = self.bids.get(i);
            let ask = self.asks.get(i);

            let bid_price = bid.map(|order| format!("{:.2}", order.price)).unwrap_or_default();
            let bid_size = bid.map(|order| format!("{:.8}", order.size)).unwrap_or_default();
            let ask_price = ask.map(|order| format!("{:.2}", order.price)).unwrap_or_default();
            let ask_size = ask.map(|order| format!("{:.8}", order.size)).unwrap_or_default();

            println!("{:<15} {:<15} | {:<15} {:<15}", bid_size, bid_price, ask_price, ask_size);
        }
        println!();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (ws_endpoint, token, ping_interval) = get_kucoin_websocket_details().await?;
    
    let connect_id = generate_connect_id();
    
    let mut ws_url = Url::parse(&ws_endpoint)?;
    ws_url.query_pairs_mut()
        .append_pair("token", &token)
        .append_pair("connectId", &connect_id);
    
    let (ws_stream, _) = connect_async(ws_url).await?;
    println!("WebSocket connection established");
    
    let (mut write, mut read) = ws_stream.split();
    
    if let Some(Ok(message)) = read.next().await {
        println!("Received welcome message: {}", message);
    }
    
    let subscribe_msg = json!({
        "id": generate_id(),
        "type": "subscribe",
        "topic": "/contractMarket/level2:ETHUSDTM",  // ETH-USDT futures market
        "privateChannel": false,
        "response": true
    });

    write.send(Message::Text(subscribe_msg.to_string())).await?;
    println!("Subscribed to ETH-USDT futures level 2 market data");
    
    let ping_interval = std::time::Duration::from_millis(ping_interval);
    let ping_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(ping_interval).await;
            let ping_msg = json!({
                "id": generate_id(),
                "type": "ping"
            });
            if let Err(e) = write.send(Message::Text(ping_msg.to_string())).await {
                eprintln!("Failed to send ping: {}", e);
                break;
            }
        }
    });
    
    let mut order_book = OrderBook::new();

    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let parsed: Value = serde_json::from_str(&text)?;

                if let Some(data) = parsed.get("data") {
                    order_book.update(data);
                    order_book.print_state();
                }
            }
            Ok(Message::Close(frame)) => {
                println!("WebSocket closed: {:?}", frame);
                break;
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
            _ => {}
        }
    }
    
    ping_task.abort();
    Ok(())
}

// Function to parse the "change" field and return the parsed market data
fn parse_market_data(change: &str) -> Option<(f64, String, f64)> {
    let parts: Vec<&str> = change.split(',').collect();
    if parts.len() == 3 {
        let price = parts[0].parse::<f64>().ok()?;
        let direction = parts[1].to_string();
        let quantity = parts[2].parse::<f64>().ok()?;
        Some((price, direction, quantity))
    } else {
        None
    }
}

// Retrieves WebSocket connection details from KuCoin's API
async fn get_kucoin_websocket_details() -> Result<(String, String, u64), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let response: Value = client.post("https://api-futures.kucoin.com/api/v1/bullet-public")
        .send()
        .await?
        .json()
        .await?;
    
    let data = response["data"].as_object().unwrap();
    let instance_server = &data["instanceServers"].as_array().unwrap()[0];
    let endpoint = instance_server["endpoint"].as_str().unwrap().to_string();
    let token = data["token"].as_str().unwrap().to_string();
    let ping_interval = instance_server["pingInterval"].as_u64().unwrap();
    
    Ok((endpoint, token, ping_interval))
}

// Generates a random connection ID
fn generate_connect_id() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

/// Generates a unique ID based on the current timestamp
fn generate_id() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string()
}
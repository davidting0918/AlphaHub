# Alpha Stability Monitor - WebSocket Service

Real-time broadcasting service for cryptocurrency token stability monitoring.

## 📋 Overview

This WebSocket service provides real-time monitoring of Binance Alpha tokens, broadcasting stability indicators and trading signals to connected clients every 5 seconds.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    WebSocket Server                         │
│                    (ws_server.py)                           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  - Connection Management                            │   │
│  │  - Message Broadcasting (every 5 seconds)           │   │
│  │  - Client Lifecycle Handling                        │   │
│  └─────────────────────────────────────────────────────┘   │
│                          ↓                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         Monitoring Service                          │   │
│  │         (monitor_service.py)                        │   │
│  │  - Token Selection & Filtering                      │   │
│  │  - Data Fetching from Binance API                   │   │
│  │  - Stability Analysis                               │   │
│  │  - Result Caching                                   │   │
│  └─────────────────────────────────────────────────────┘   │
│                          ↓                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │      Alpha Stability Monitor Core                   │   │
│  │      (bn_alpha_monitor package)                     │   │
│  │  - Volatility Indicators                            │   │
│  │  - Directional Indicators                           │   │
│  │  - Real-time Analysis                               │   │
│  │  - Composite Scoring                                │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                             ↓
                ┌────────────────────────┐
                │   WebSocket Clients    │
                │  (client_example.py)   │
                └────────────────────────┘
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the Server

```bash
# From the project root
cd src/webservice
python ws_server.py
```

Or use the convenience script:

```bash
python src/run_websocket_server.py
```

The server will start on `ws://0.0.0.0:8765` by default.

### 3. Connect a Client

In a separate terminal:

```bash
python src/webservice/client_example.py
```

Or connect from a different machine:

```bash
python src/webservice/client_example.py ws://YOUR_SERVER_IP:8765
```

## 📊 Message Format

### Connection Message
```json
{
  "type": "connection",
  "status": "connected",
  "message": "Connected to Alpha Stability Monitor",
  "timestamp": 1699283742000,
  "client_id": 140234567890
}
```

### Monitoring Update Message
```json
{
  "type": "monitoring_update",
  "broadcast_time": 1699283742000,
  "data": {
    "timestamp": 1699283742000,
    "total_symbols": 15,
    "successful": 14,
    "failed": 1,
    "summary": {
      "green_signals": 5,
      "yellow_signals": 6,
      "red_signals": 3
    },
    "results": [
      {
        "symbol": "BTCUSDT",
        "alpha_id": "BTCUSDT",
        "timestamp": 1699283742000,
        "signal": "green",
        "composite_score": 85.5,
        "recommendation": "can trade - market is stable",
        "metrics": {
          "rolling_volatility": {
            "value": 0.0234,
            "score": 88.5,
            "weight": 0.30
          },
          "atr": {
            "value": 0.0189,
            "score": 82.3,
            "weight": 0.25
          }
          // ... more metrics
        }
      }
      // ... more results
    ]
  }
}
```

### Error Message
```json
{
  "type": "error",
  "message": "Monitoring error: Connection timeout",
  "timestamp": 1699283742000
}
```

## 🔧 Configuration

### Server Configuration

Edit `ws_server.py` to change server settings:

```python
# Default settings
server = AlphaMonitorWebSocket(
    host="0.0.0.0",  # Listen on all interfaces
    port=8765        # WebSocket port
)
```

### Monitoring Service Configuration

Edit `monitor_service.py` or pass parameters to `MonitoringService`:

```python
service = MonitoringService(
    min_multiplier=1.0,      # Minimum token multiplier
    min_volume=0.0,          # Minimum 24h volume
    top_n_tokens=20          # Limit to top N tokens (None = all)
)
```

## 📡 Client API

### Sending Messages to Server

#### Ping Request
```json
{
  "type": "ping"
}
```

Response:
```json
{
  "type": "pong",
  "timestamp": 1699283742000
}
```

#### Status Request
```json
{
  "type": "get_status"
}
```

Response:
```json
{
  "type": "status",
  "is_monitoring": true,
  "total_clients": 3,
  "timestamp": 1699283742000
}
```

## 🌐 Integration Examples

### Python Client (websockets)

```python
import asyncio
import websockets
import json

async def monitor_client():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            data = json.loads(message)
            if data["type"] == "monitoring_update":
                print(f"Received update: {data['data']['summary']}")

asyncio.run(monitor_client())
```

### JavaScript Client (Browser)

```javascript
const ws = new WebSocket('ws://localhost:8765');

ws.onopen = () => {
    console.log('Connected to Alpha Monitor');
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    
    if (data.type === 'monitoring_update') {
        console.log('Monitoring Update:', data.data);
        // Update your UI here
    }
};

ws.onerror = (error) => {
    console.error('WebSocket Error:', error);
};
```

### Python Client (websocket-client library)

```python
import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    if data["type"] == "monitoring_update":
        print(f"Green: {data['data']['summary']['green_signals']}")
        print(f"Yellow: {data['data']['summary']['yellow_signals']}")
        print(f"Red: {data['data']['summary']['red_signals']}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected")

ws = websocket.WebSocketApp(
    "ws://localhost:8765",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()
```

## 🔍 Monitoring Features

### Signal Levels

- **🟢 GREEN**: Stable market conditions, safe to trade
  - Low volatility
  - Consistent trends
  - Minimal price jumps

- **🟡 YELLOW**: Moderate volatility, trade with caution
  - Medium volatility
  - Some price fluctuations
  - Moderate trading activity

- **🔴 RED**: High volatility or insufficient data, avoid trading
  - High volatility
  - Erratic price movements
  - Illiquidity or data issues

### Metrics Analyzed

1. **Rolling Volatility** (30% weight)
   - 15-minute price standard deviation

2. **Average True Range (ATR)** (25% weight)
   - Price movement range indicator

3. **Price Range** (15% weight)
   - High-low price spread

4. **Trend Strength** (10% weight)
   - Consistency of price direction

5. **Price Jump Frequency** (10% weight)
   - Sudden price change detection

6. **Real-time Deviation** (10% weight)
   - Current vs. average price comparison

## 🛠️ Troubleshooting

### Server won't start

- **Port already in use**: Change the port in `ws_server.py`
  ```python
  server = AlphaMonitorWebSocket(host="0.0.0.0", port=8766)
  ```

- **Missing dependencies**: Run `pip install -r requirements.txt`

### Client can't connect

- **Check server is running**: You should see "Server is running" message
- **Firewall**: Ensure port 8765 is open
- **IP address**: Use `0.0.0.0` for local connections or server's IP for remote

### No data received

- **API issues**: Check Binance API connectivity
- **Token filtering**: Adjust `min_multiplier` and `min_volume` in `monitor_service.py`
- **Server logs**: Check console output for errors

## 📝 Logging

The service uses Python's logging module. To adjust log level:

```python
import logging

# In ws_server.py or monitor_service.py
logging.basicConfig(level=logging.DEBUG)  # Show all logs
logging.basicConfig(level=logging.INFO)   # Default
logging.basicConfig(level=logging.WARNING) # Only warnings and errors
```

## 🔒 Security Considerations

### Production Deployment

1. **Use SSL/TLS**: Upgrade to WSS (secure WebSocket)
   ```python
   import ssl
   ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
   ssl_context.load_cert_chain('cert.pem', 'key.pem')
   
   async with websockets.serve(handler, host, port, ssl=ssl_context):
       ...
   ```

2. **Authentication**: Implement token-based authentication
3. **Rate Limiting**: Add connection and message rate limits
4. **Firewall**: Restrict access to known IPs

## 📦 Files

- `ws_server.py` - WebSocket server implementation
- `monitor_service.py` - Monitoring logic and data fetching
- `client_example.py` - Example client for testing
- `__init__.py` - Package initialization
- `README.md` - This file

## 🤝 Contributing

When modifying the service:

1. Keep concerns separated (server, service, business logic)
2. Add logging for debugging
3. Handle errors gracefully
4. Update this README

## 📄 License

Part of the AlphaHub project.


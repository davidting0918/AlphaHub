"""
WebSocket Client Example

Example client for connecting to the Alpha Stability Monitor WebSocket service
and receiving real-time monitoring updates.
"""

import asyncio
import json
import websockets
from datetime import datetime as dt
import sys


class MonitorClient:
    """
    Simple WebSocket client for Alpha Stability Monitor
    
    Connects to the server and displays incoming monitoring updates.
    """
    
    def __init__(self, server_uri: str = "ws://localhost:8765"):
        """
        Initialize client
        
        Args:
            server_uri: WebSocket server URI (default: ws://localhost:8765)
        """
        self.server_uri = server_uri
        self.websocket = None
        self.is_connected = False
    
    async def connect(self):
        """
        Connect to the WebSocket server
        """
        print(f"🔗 Connecting to {self.server_uri}...")
        
        try:
            self.websocket = await websockets.connect(self.server_uri)
            self.is_connected = True
            print("✓ Connected successfully!")
            print("=" * 70)
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            raise
    
    async def listen(self):
        """
        Listen for messages from the server
        """
        if not self.is_connected:
            raise RuntimeError("Not connected to server")
        
        try:
            async for message in self.websocket:
                await self.handle_message(message)
        except websockets.exceptions.ConnectionClosed:
            print("\n⚠ Connection closed by server")
            self.is_connected = False
        except Exception as e:
            print(f"\n❌ Error: {e}")
            self.is_connected = False
    
    async def handle_message(self, message: str):
        """
        Handle incoming message from server
        
        Args:
            message: JSON string message
        """
        try:
            data = json.loads(message)
            msg_type = data.get("type", "unknown")
            
            if msg_type == "connection":
                self._handle_connection(data)
            elif msg_type == "monitoring_update":
                self._handle_monitoring_update(data)
            elif msg_type == "error":
                self._handle_error(data)
            elif msg_type == "pong":
                print(f"🏓 Pong received at {self._format_time(data['timestamp'])}")
            else:
                print(f"❓ Unknown message type: {msg_type}")
        
        except json.JSONDecodeError:
            print(f"⚠ Invalid JSON received: {message}")
        except Exception as e:
            print(f"❌ Error handling message: {e}")
    
    def _handle_connection(self, data: dict):
        """Handle connection acknowledgment"""
        print(f"\n📨 {data['message']}")
        print(f"   Client ID: {data['client_id']}")
        print(f"   Time: {self._format_time(data['timestamp'])}")
        print("=" * 70)
    
    def _handle_monitoring_update(self, data: dict):
        """Handle monitoring update message"""
        monitoring_data = data['data']
        timestamp = monitoring_data['timestamp']
        summary = monitoring_data['summary']
        
        print(f"\n📊 MONITORING UPDATE - {self._format_time(timestamp)}")
        print("=" * 70)
        print(f"   Total Symbols:  {monitoring_data['total_symbols']}")
        print(f"   Successful:     {monitoring_data['successful']}")
        print(f"   Failed:         {monitoring_data['failed']}")
        print(f"\n   Signal Distribution:")
        print(f"      🟢 Green:  {summary['green_signals']}")
        print(f"      🟡 Yellow: {summary['yellow_signals']}")
        print(f"      🔴 Red:    {summary['red_signals']}")
        
        # Show top 5 tokens by score
        results = monitoring_data['results']
        valid_results = [r for r in results if 'error' not in r]
        valid_results.sort(key=lambda x: x['composite_score'], reverse=True)
        
        if valid_results:
            print(f"\n   📈 Top 5 Tokens by Stability Score:")
            for i, result in enumerate(valid_results[:5], 1):
                signal_emoji = {
                    'green': '🟢',
                    'yellow': '🟡',
                    'red': '🔴'
                }.get(result['signal'], '⚪')
                
                print(
                    f"      {i}. {signal_emoji} {result['symbol']:12s} | "
                    f"Score: {result['composite_score']:5.1f}/100 | "
                    f"Signal: {result['signal'].upper():6s}"
                )
        
        print("=" * 70)
    
    def _handle_error(self, data: dict):
        """Handle error message"""
        print(f"\n❌ ERROR from server:")
        print(f"   {data['message']}")
        print(f"   Time: {self._format_time(data['timestamp'])}")
    
    async def send_ping(self):
        """Send ping to server"""
        if self.is_connected:
            await self.websocket.send(json.dumps({"type": "ping"}))
    
    async def request_status(self):
        """Request server status"""
        if self.is_connected:
            await self.websocket.send(json.dumps({"type": "get_status"}))
    
    async def close(self):
        """Close connection"""
        if self.websocket:
            await self.websocket.close()
            self.is_connected = False
            print("\n✓ Connection closed")
    
    @staticmethod
    def _format_time(timestamp_ms: int) -> str:
        """Format timestamp for display"""
        return dt.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S')


async def main():
    """
    Main client function
    """
    # Parse command line arguments
    server_uri = "ws://localhost:8765"
    if len(sys.argv) > 1:
        server_uri = sys.argv[1]
    
    print("=" * 70)
    print("Alpha Stability Monitor - WebSocket Client")
    print("=" * 70)
    
    client = MonitorClient(server_uri)
    
    try:
        await client.connect()
        await client.listen()
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())


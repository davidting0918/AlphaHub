"""
WebSocket Server - Alpha Stability Monitor Broadcasting

Manages WebSocket connections and broadcasts monitoring results to all subscribers.
"""

import asyncio
import json
import logging
import websockets
from typing import Set, Optional
from datetime import datetime as dt
from websockets.server import WebSocketServerProtocol

from .monitor_service import MonitoringService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class AlphaMonitorWebSocket:
    """
    WebSocket server for broadcasting alpha stability monitoring results
    
    Features:
    - Auto-reconnection handling
    - Concurrent client management
    - Periodic data broadcasting (every 5 seconds)
    - Clean connection lifecycle management
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        """
        Initialize WebSocket server
        
        Args:
            host: Server host address (default: 0.0.0.0 for all interfaces)
            port: Server port (default: 8765)
        """
        self.host = host
        self.port = port
        self.clients: Set[WebSocketServerProtocol] = set()
        self.monitor_service: Optional[MonitoringService] = None
        self.broadcast_task: Optional[asyncio.Task] = None
        self.is_running = False
    
    async def register_client(self, websocket: WebSocketServerProtocol):
        """
        Register a new WebSocket client
        
        Args:
            websocket: WebSocket connection instance
        """
        self.clients.add(websocket)
        client_id = id(websocket)
        logger.info(f"✓ Client connected [ID: {client_id}] - Total clients: {len(self.clients)}")
        
        # Send welcome message
        welcome_msg = {
            "type": "connection",
            "status": "connected",
            "message": "Connected to Alpha Stability Monitor",
            "timestamp": int(dt.now().timestamp() * 1000),
            "client_id": client_id
        }
        await websocket.send(json.dumps(welcome_msg))
    
    async def unregister_client(self, websocket: WebSocketServerProtocol):
        """
        Unregister a WebSocket client
        
        Args:
            websocket: WebSocket connection instance
        """
        self.clients.discard(websocket)
        client_id = id(websocket)
        logger.info(f"✗ Client disconnected [ID: {client_id}] - Total clients: {len(self.clients)}")
    
    async def broadcast_message(self, message: dict):
        """
        Broadcast a message to all connected clients
        
        Args:
            message: Dictionary to be sent as JSON
        """
        if not self.clients:
            return
        
        message_json = json.dumps(message)
        disconnected_clients = set()
        
        # Send to all clients concurrently
        async def send_to_client(client):
            try:
                await client.send(message_json)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"Error sending to client {id(client)}: {e}")
                disconnected_clients.add(client)
        
        await asyncio.gather(
            *[send_to_client(client) for client in self.clients],
            return_exceptions=True
        )
        
        # Clean up disconnected clients
        for client in disconnected_clients:
            await self.unregister_client(client)
    
    async def broadcast_monitoring_loop(self):
        """
        Main loop: fetch monitoring data and broadcast every 5 seconds
        """
        logger.info("🔄 Starting monitoring broadcast loop (interval: 5s)")
        
        while self.is_running:
            try:
                # Fetch latest monitoring results
                batch_result = await self.monitor_service.get_monitoring_results()
                
                if batch_result:
                    # Prepare broadcast message
                    message = {
                        "type": "monitoring_update",
                        "data": batch_result,
                        "broadcast_time": int(dt.now().timestamp() * 1000)
                    }
                    
                    # Broadcast to all clients
                    await self.broadcast_message(message)
                    
                    logger.info(
                        f"📡 Broadcasted update to {len(self.clients)} clients | "
                        f"Signals: 🟢{batch_result['summary']['green_signals']} "
                        f"🟡{batch_result['summary']['yellow_signals']} "
                        f"🔴{batch_result['summary']['red_signals']}"
                    )
                else:
                    logger.warning("⚠ No monitoring data available")
                
            except Exception as e:
                logger.error(f"❌ Error in broadcast loop: {e}")
                # Send error notification to clients
                error_msg = {
                    "type": "error",
                    "message": f"Monitoring error: {str(e)}",
                    "timestamp": int(dt.now().timestamp() * 1000)
                }
                await self.broadcast_message(error_msg)
            
            # Wait for next iteration
            await asyncio.sleep(5)
    
    async def handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """
        Handle individual WebSocket client connection
        
        Args:
            websocket: WebSocket connection instance
            path: Connection path
        """
        await self.register_client(websocket)
        
        try:
            # Keep connection alive and handle incoming messages
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    # Handle different message types
                    if data.get("type") == "ping":
                        await websocket.send(json.dumps({
                            "type": "pong",
                            "timestamp": int(dt.now().timestamp() * 1000)
                        }))
                    elif data.get("type") == "get_status":
                        status = {
                            "type": "status",
                            "is_monitoring": self.monitor_service.is_running if self.monitor_service else False,
                            "total_clients": len(self.clients),
                            "timestamp": int(dt.now().timestamp() * 1000)
                        }
                        await websocket.send(json.dumps(status))
                    else:
                        logger.debug(f"Unknown message type from client {id(websocket)}: {data}")
                        
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from client {id(websocket)}")
                except Exception as e:
                    logger.error(f"Error handling message from client {id(websocket)}: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.unregister_client(websocket)
    
    async def start(self):
        """
        Start the WebSocket server and monitoring service
        """
        logger.info("=" * 70)
        logger.info("🚀 Alpha Stability Monitor WebSocket Service")
        logger.info("=" * 70)
        
        # Initialize monitoring service
        logger.info("🔧 Initializing monitoring service...")
        self.monitor_service = MonitoringService()
        await self.monitor_service.start()
        
        # Start broadcast loop
        self.is_running = True
        self.broadcast_task = asyncio.create_task(self.broadcast_monitoring_loop())
        
        # Start WebSocket server
        logger.info(f"🌐 Starting WebSocket server on ws://{self.host}:{self.port}")
        async with websockets.serve(self.handle_client, self.host, self.port):
            logger.info("✓ Server is running and accepting connections")
            logger.info("=" * 70)
            
            # Keep server running
            await asyncio.Future()  # Run forever
    
    async def stop(self):
        """
        Gracefully stop the WebSocket server and monitoring service
        """
        logger.info("🛑 Shutting down server...")
        
        # Stop broadcast loop
        self.is_running = False
        if self.broadcast_task:
            self.broadcast_task.cancel()
            try:
                await self.broadcast_task
            except asyncio.CancelledError:
                pass
        
        # Close all client connections
        if self.clients:
            await asyncio.gather(
                *[client.close() for client in self.clients],
                return_exceptions=True
            )
        
        # Stop monitoring service
        if self.monitor_service:
            await self.monitor_service.stop()
        
        logger.info("✓ Server stopped")


async def main():
    """
    Main entry point for running the WebSocket server
    """
    server = AlphaMonitorWebSocket(host="0.0.0.0", port=8765)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("\n⚠ Shutdown signal received")
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())


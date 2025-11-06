"""
Quick Test Script for WebSocket Service

Tests both server and client functionality in a single script.
"""

import asyncio
import json
import websockets
from datetime import datetime as dt


async def test_client(uri: str, duration: int = 20):
    """
    Test client that connects and listens for updates
    
    Args:
        uri: WebSocket server URI
        duration: How long to run the test (seconds)
    """
    print("\n" + "=" * 70)
    print("🧪 WebSocket Client Test")
    print("=" * 70)
    print(f"🔗 Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected successfully!\n")
            
            updates_received = 0
            start_time = asyncio.get_event_loop().time()
            
            # Listen for messages
            while True:
                # Check if duration exceeded
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > duration:
                    print(f"\n⏰ Test duration ({duration}s) reached")
                    break
                
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=duration - elapsed
                    )
                    
                    data = json.loads(message)
                    msg_type = data.get("type")
                    
                    if msg_type == "connection":
                        print(f"📨 Connection acknowledged")
                        print(f"   Client ID: {data['client_id']}")
                    
                    elif msg_type == "monitoring_update":
                        updates_received += 1
                        monitoring_data = data['data']
                        summary = monitoring_data['summary']
                        
                        print(f"\n📊 Update #{updates_received} - {dt.now().strftime('%H:%M:%S')}")
                        print(f"   Symbols: {monitoring_data['successful']}/{monitoring_data['total_symbols']}")
                        print(f"   Signals: 🟢{summary['green_signals']} 🟡{summary['yellow_signals']} 🔴{summary['red_signals']}")
                    
                    elif msg_type == "error":
                        print(f"\n❌ Server error: {data['message']}")
                    
                except asyncio.TimeoutError:
                    print(f"\n⏰ No message received within {duration}s")
                    break
                except json.JSONDecodeError:
                    print(f"\n⚠️  Invalid JSON received")
                except Exception as e:
                    print(f"\n❌ Error: {e}")
                    break
            
            # Test summary
            print("\n" + "=" * 70)
            print("📈 Test Summary")
            print("=" * 70)
            print(f"   Duration: {elapsed:.1f}s")
            print(f"   Updates Received: {updates_received}")
            print(f"   Expected Updates: ~{int(elapsed / 5)} (every 5s)")
            
            if updates_received >= int(elapsed / 5) - 1:
                print(f"   Status: ✅ PASS")
            else:
                print(f"   Status: ⚠️  INCOMPLETE (fewer updates than expected)")
            
            print("=" * 70)
    
    except websockets.exceptions.WebSocketException as e:
        print(f"❌ WebSocket Error: {e}")
        print("\n💡 Tip: Make sure the server is running:")
        print("   python src/webservice/ws_server.py")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True


async def main():
    """Main test function"""
    import sys
    
    # Parse arguments
    uri = "ws://localhost:8765"
    duration = 20
    
    if len(sys.argv) > 1:
        uri = sys.argv[1]
    if len(sys.argv) > 2:
        duration = int(sys.argv[2])
    
    print(f"\n🎯 Test Configuration:")
    print(f"   Server URI: {uri}")
    print(f"   Test Duration: {duration}s")
    
    success = await test_client(uri, duration)
    
    if success:
        print("\n✅ Test completed successfully!\n")
        return 0
    else:
        print("\n❌ Test failed\n")
        return 1


if __name__ == "__main__":
    import sys
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}\n")
        sys.exit(1)


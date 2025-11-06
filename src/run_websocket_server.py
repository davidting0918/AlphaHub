#!/usr/bin/env python3
"""
Convenience script to run the Alpha Stability Monitor WebSocket Server

Usage:
    python run_websocket_server.py [--host HOST] [--port PORT]
"""

import asyncio
import sys
import os
import argparse

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from webservice import AlphaMonitorWebSocket


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Alpha Stability Monitor WebSocket Server'
    )
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host address to bind to (default: 0.0.0.0)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8765,
        help='Port to listen on (default: 8765)'
    )
    parser.add_argument(
        '--min-multiplier',
        type=float,
        default=1.0,
        help='Minimum token multiplier for filtering (default: 1.0)'
    )
    parser.add_argument(
        '--min-volume',
        type=float,
        default=0.0,
        help='Minimum 24h volume for filtering (default: 0.0)'
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=None,
        help='Limit to top N tokens by volume (default: None = all)'
    )
    
    return parser.parse_args()


async def main():
    """Main entry point"""
    args = parse_arguments()
    
    print("\n" + "=" * 70)
    print("🚀 Starting Alpha Stability Monitor WebSocket Service")
    print("=" * 70)
    print(f"\n📍 Configuration:")
    print(f"   Host: {args.host}")
    print(f"   Port: {args.port}")
    print(f"   Min Multiplier: {args.min_multiplier}")
    print(f"   Min Volume: ${args.min_volume:,.0f}")
    print(f"   Top N Tokens: {args.top_n if args.top_n else 'All'}")
    print("\n" + "=" * 70 + "\n")
    
    # Create server instance
    server = AlphaMonitorWebSocket(host=args.host, port=args.port)
    
    # Note: Monitoring service configuration will need to be passed through
    # For now, it uses defaults. To customize, modify AlphaMonitorWebSocket
    # to accept and pass these parameters to MonitoringService
    
    try:
        await server.start()
    except KeyboardInterrupt:
        print("\n\n⚠️  Shutdown signal received")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await server.stop()
        print("\n✅ Server shutdown complete\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        sys.exit(1)


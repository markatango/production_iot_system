import asyncio
import ssl
import struct
import time
import uuid
import logging
import argparse
import json
from typing import Optional, Dict, Callable, Tuple
from enum import Enum

from iot_client import CompatibleMQTTClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QoS(Enum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2

class ConnectionState(Enum):
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    DISCONNECTING = 3
    
async def publish_mode(args):
    """Publish mode - send a single message"""
    client = CompatibleMQTTClient(client_id=args.client_id)
    
    async def on_hello(topic: str, payload: bytes):
        print("✅ Authentication successful (HELLO received)")
    
    client.on_hello = on_hello
    
    # Connect
    port = args.ssl_port if args.ssl else args.port
    success = await client.connect(
        host=args.host,
        port=port,
        use_ssl=args.ssl,
        cert_file=args.cert,
        username=args.username,
        password=args.password,
        verify_cert=False
    )
    
    if not success:
        print("❌ Failed to connect to broker")
        return
    
    # Publish
    topic, message = args.topic, args.message
    qos = QoS(args.qos)
    logger.debug(f"Publishing to topic '{topic}' with QoS {qos} and N={args.n}")
    if args.n > 1:
        for i in range(args.n):
            message = f"{message} {i+1}"
            success = await client.publish(
                topic=topic,
                payload=message.encode('utf-8'),
                qos=qos,
                retain=args.retain
            )
            if success:
                print(f"✅ Published message to '{topic}': {message}")
            else:
                print(f"❌ Failed to publish message to '{topic}'")
            await asyncio.sleep(args.delay)
    else:
        # Single message publish
        if not topic:
            topic = args.topic
        if not message:
            message = args.message
    
        success = await client.publish(
            topic=topic,
            payload=message.encode('utf-8'),
            qos=qos,
            retain=args.retain
        )
    
    if success:
        print(f"✅ Published message to '{topic}': {message}")
    else:
        print(f"❌ Failed to publish message to '{topic}'")
    
    await client.disconnect()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Compatible IoT MQTT Client')
    # parser.add_argument('--interactive', '-i', action='store_true', 
    #                    help='Run in interactive mode')
    parser.add_argument('--host', default='localhost', 
                       help='Broker host (default: localhost)')
    parser.add_argument('--port', type=int, default=1883, 
                       help='Broker port (default: 1883)')
    parser.add_argument('--ssl', action='store_true', 
                       help='Use SSL/TLS connection')
    parser.add_argument('--ssl-port', type=int, default=8883, 
                       help='SSL port (default: 8883)')
    parser.add_argument('--cert', 
                       help='SSL certificate file')
    parser.add_argument('--client-id', 
                       help='MQTT client ID')
    parser.add_argument('--username', 
                       help='MQTT username')
    parser.add_argument('--password', 
                       help='MQTT password')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    

    # Publishing options
    # parser.add_argument('--publish', '-p', nargs=2, metavar=('TOPIC', 'MESSAGE'),
    #                    help='Publish message to topic')
    parser.add_argument('--topic', '-t', default="emqx/esp32",
                       help='Publish message to topic')
    parser.add_argument('--message', '-m', default="Hello, MQTT!",
                       help='Message to publish')
    parser.add_argument('--qos', type=int, choices=[0, 1, 2], default=0,
                       help='QoS level (default: 1)')
    parser.add_argument('--retain', action='store_true',
                       help='Set retain flag for published messages')
    parser.add_argument('--n', type=int, default=10,
                       help='Number of messages to publish (default: 10)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between messages in seconds (default: 1.0)')
    
    args = parser.parse_args()
    print(f"{args}")
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        asyncio.run(publish_mode(args))

    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Error: {e}")
        import sys
        sys.exit(1)

if __name__ == '__main__':
    main()
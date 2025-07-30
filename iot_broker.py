#!/usr/bin/env python3
"""
IoT MQTT Broker - Compatible with EMQX (SSL/TLS Support)
A Python implementation of an MQTT broker that mimics broker.emqx.io behavior
"""

import asyncio
import logging
import json
import time
import weakref
from typing import Dict, Set, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import socket
import struct
import uuid
import ssl
import os
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime
import ipaddress

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QoS(Enum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2

class MessageType(Enum):
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14

@dataclass
class MQTTMessage:
    topic: str
    payload: bytes
    qos: QoS = QoS.AT_MOST_ONCE
    retain: bool = False
    timestamp: float = field(default_factory=time.time)
    message_id: Optional[int] = None

@dataclass
class Subscription:
    topic_filter: str
    qos: QoS
    client_id: str

class TopicMatcher:
    """Handles MQTT topic matching with wildcards (+, #)"""
    
    @staticmethod
    def matches(topic_filter: str, topic: str) -> bool:
        filter_parts = topic_filter.split('/')
        topic_parts = topic.split('/')
        
        return TopicMatcher._match_parts(filter_parts, topic_parts)
    
    @staticmethod
    def _match_parts(filter_parts: List[str], topic_parts: List[str]) -> bool:
        fi = ti = 0
        
        while fi < len(filter_parts) and ti < len(topic_parts):
            if filter_parts[fi] == '#':
                return True
            elif filter_parts[fi] == '+':
                fi += 1
                ti += 1
            elif filter_parts[fi] == topic_parts[ti]:
                fi += 1
                ti += 1
            else:
                return False
        
        # Handle remaining parts
        if fi < len(filter_parts):
            return len(filter_parts) - fi == 1 and filter_parts[fi] == '#'
        
        return ti == len(topic_parts)

class MQTTClient:
    """Represents a connected MQTT client"""
    
    def __init__(self, client_id: str, transport: asyncio.Transport, protocol: 'MQTTProtocol'):
        self.client_id = client_id
        self.transport = transport
        self.protocol = protocol
        self.subscriptions: Dict[str, QoS] = {}
        self.last_seen = time.time()
        self.keep_alive = 60
        self.clean_session = True
        self.will_message: Optional[MQTTMessage] = None
        self.pending_messages: Dict[int, MQTTMessage] = {}
        self.next_message_id = 1
        self.connected = True
        
    def get_next_message_id(self) -> int:
        msg_id = self.next_message_id
        self.next_message_id = (self.next_message_id % 65535) + 1
        return msg_id
    
    def update_last_seen(self):
        self.last_seen = time.time()
    
    def is_alive(self) -> bool:
        if self.keep_alive == 0:
            return True
        return time.time() - self.last_seen < self.keep_alive * 1.5
    
    def disconnect(self):
        self.connected = False
        if self.transport and not self.transport.is_closing():
            self.transport.close()

class MQTTProtocol(asyncio.Protocol):
    """MQTT Protocol handler with SSL/TLS support"""
    
    def __init__(self, broker: 'MQTTBroker', use_ssl: bool = False):
        self.broker = broker
        self.use_ssl = use_ssl
        self.transport: Optional[asyncio.Transport] = None
        self.client: Optional[MQTTClient] = None
        self.buffer = bytearray()
        
    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport
        peer = transport.get_extra_info('peername')
        ssl_object = transport.get_extra_info('ssl_object')
        
        if ssl_object:
            cipher = ssl_object.cipher()
            protocol = ssl_object.version()
            logger.info(f"SSL connection from {peer} using {protocol} with {cipher}")
            self.broker.stats['ssl_connections'] += 1
        else:
            logger.info(f"Plain connection from {peer}")
            self.broker.stats['plain_connections'] += 1
        
    def connection_lost(self, exc):
        if self.client:
            logger.info(f"Client {self.client.client_id} disconnected")
            self.broker.disconnect_client(self.client.client_id)
    
    def data_received(self, data: bytes):
        self.buffer.extend(data)
        
        while len(self.buffer) >= 2:
            try:
                # Parse fixed header
                msg_type = (self.buffer[0] >> 4) & 0x0F
                flags = self.buffer[0] & 0x0F
                
                # Parse remaining length
                remaining_length, length_bytes = self._decode_remaining_length(1)
                if remaining_length is None:
                    break  # Need more data
                    
                total_length = 1 + length_bytes + remaining_length
                if len(self.buffer) < total_length:
                    break  # Need more data
                    
                # Extract complete message
                message_data = bytes(self.buffer[:total_length])
                self.buffer = self.buffer[total_length:]
                
                # Process message
                asyncio.create_task(self._process_message(msg_type, flags, message_data))
                
            except Exception as e:
                logger.error(f"Error in data_received: {e}")
                logger.debug(f"Buffer content: {self.buffer[:50].hex()}...")
                # Clear buffer on error to prevent infinite loop
                self.buffer.clear()
                break
    
    def _decode_remaining_length(self, start_pos: int) -> Tuple[Optional[int], int]:
        """Decode MQTT remaining length field"""
        if start_pos >= len(self.buffer):
            return None, 0
            
        length = 0
        multiplier = 1
        pos = start_pos
        bytes_used = 0
        
        while pos < len(self.buffer) and bytes_used < 4:  # Max 4 bytes for remaining length
            byte = self.buffer[pos]
            length += (byte & 0x7F) * multiplier
            pos += 1
            bytes_used += 1
            
            if (byte & 0x80) == 0:
                return length, bytes_used
                
            multiplier *= 128
            if multiplier > 128 * 128 * 128:
                raise ValueError("Remaining length exceeds maximum")
        
        # Need more data if we haven't found the end
        if bytes_used == 4 and pos < len(self.buffer) and (self.buffer[pos-1] & 0x80) != 0:
            raise ValueError("Remaining length exceeds 4 bytes")
        
        return None, 0  # Need more data
    
    def _encode_remaining_length(self, length: int) -> bytes:
        """Encode MQTT remaining length field"""
        result = bytearray()
        while True:
            byte = length % 128
            length //= 128
            if length > 0:
                byte |= 0x80
            result.append(byte)
            if length == 0:
                break
        return bytes(result)
    
    async def _process_message(self, msg_type: int, flags: int, data: bytes):
        """Process incoming MQTT message"""
        try:
            if self.broker.log_raw_data:
                logger.debug(f"Processing message type {msg_type}, flags {flags:02x}, data length {len(data)}")
                logger.debug(f"Raw data: {data[:min(50, len(data))].hex()}...")
            
            if msg_type == MessageType.CONNECT.value:
                await self._handle_connect(data)
            elif msg_type == MessageType.PUBLISH.value:
                await self._handle_publish(flags, data)
            elif msg_type == MessageType.SUBSCRIBE.value:
                await self._handle_subscribe(data)
            elif msg_type == MessageType.UNSUBSCRIBE.value:
                await self._handle_unsubscribe(data)
            elif msg_type == MessageType.PINGREQ.value:
                await self._handle_pingreq()
            elif msg_type == MessageType.DISCONNECT.value:
                await self._handle_disconnect()
            elif msg_type == MessageType.PUBACK.value:
                await self._handle_puback(data)
            else:
                logger.warning(f"Unhandled message type: {msg_type}")
        except Exception as e:
            logger.error(f"Error processing message type {msg_type}: {e}")
            if self.broker.log_raw_data:
                logger.debug(f"Exception details:", exc_info=True)
    
    async def _handle_connect(self, data: bytes):
        """Handle CONNECT message"""
        try:
            # Manual debug of the remaining length parsing
            logger.debug(f"CONNECT data hex: {data.hex()}")
            
            # The remaining length starts at position 1
            remaining_length = 0
            multiplier = 1
            pos = 1
            length_bytes = 0
            
            while pos < len(data) and length_bytes < 4:
                byte = data[pos]
                remaining_length += (byte & 0x7F) * multiplier
                length_bytes += 1
                pos += 1
                
                logger.debug(f"Remaining length byte {length_bytes}: 0x{byte:02x}, value: {byte & 0x7F}, multiplier: {multiplier}")
                
                if (byte & 0x80) == 0:
                    break
                multiplier *= 128
            
            logger.debug(f"Parsed remaining length: {remaining_length}, used {length_bytes} bytes")
            
            # Start parsing variable header
            var_header_start = 1 + length_bytes
            pos = var_header_start
            
            if pos >= len(data):
                logger.error("CONNECT message too short")
                return
            
            # Protocol name
            if pos + 2 > len(data):
                logger.error("CONNECT message missing protocol name length")
                return
            proto_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            
            logger.debug(f"Protocol name length: {proto_len}")
            
            if pos + proto_len > len(data):
                logger.error(f"CONNECT message protocol name truncated: need {proto_len} bytes, have {len(data) - pos}")
                logger.debug(f"Data length: {len(data)}, pos: {pos}, proto_len: {proto_len}")
                return
            protocol_name = data[pos:pos+proto_len].decode('utf-8')
            pos += proto_len
            
            logger.debug(f"Protocol name: {protocol_name}, length: {proto_len}")
            
            # Protocol version
            if pos >= len(data):
                logger.error("CONNECT message missing protocol version")
                return
            protocol_version = data[pos]
            pos += 1
            
            # Connect flags
            if pos >= len(data):
                logger.error("CONNECT message missing connect flags")
                return
            connect_flags = data[pos]
            pos += 1
            
            clean_session = bool(connect_flags & 0x02)
            will_flag = bool(connect_flags & 0x04)
            username_flag = bool(connect_flags & 0x80)
            password_flag = bool(connect_flags & 0x40)
            
            # Keep alive
            if pos + 2 > len(data):
                logger.error("CONNECT message missing keep alive")
                return
            keep_alive = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            
            # Client ID
            if pos + 2 > len(data):
                logger.error("CONNECT message missing client ID length")
                return
            client_id_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            
            if pos + client_id_len > len(data):
                logger.error(f"CONNECT message client ID truncated: need {client_id_len} bytes, have {len(data) - pos}")
                return
            client_id = data[pos:pos+client_id_len].decode('utf-8')
            pos += client_id_len
            
            if not client_id:
                client_id = f"mqtt_client_{uuid.uuid4().hex[:8]}"
            
            logger.debug(f"Client ID: {client_id}, clean_session: {clean_session}")
            
            # Will message (if present)
            will_message = None
            if will_flag:
                if pos + 2 > len(data):
                    logger.error("CONNECT message missing will topic length")
                    return
                will_topic_len = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
                
                if pos + will_topic_len > len(data):
                    logger.error("CONNECT message will topic truncated")
                    return
                will_topic = data[pos:pos+will_topic_len].decode('utf-8')
                pos += will_topic_len
                
                if pos + 2 > len(data):
                    logger.error("CONNECT message missing will payload length")
                    return
                will_payload_len = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
                
                if pos + will_payload_len > len(data):
                    logger.error("CONNECT message will payload truncated")
                    return
                will_payload = data[pos:pos+will_payload_len]
                pos += will_payload_len
                
                will_qos = QoS((connect_flags >> 3) & 0x03)
                will_retain = bool(connect_flags & 0x20)
                
                will_message = MQTTMessage(will_topic, will_payload, will_qos, will_retain)
            
            # Username and password (if present)
            username = password = None
            if username_flag:
                if pos + 2 > len(data):
                    logger.error("CONNECT message missing username length")
                    return
                username_len = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
                
                if pos + username_len > len(data):
                    logger.error("CONNECT message username truncated")
                    return
                username = data[pos:pos+username_len].decode('utf-8')
                pos += username_len
                
            if password_flag:
                if pos + 2 > len(data):
                    logger.error("CONNECT message missing password length")
                    return
                password_len = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
                
                if pos + password_len > len(data):
                    logger.error("CONNECT message password truncated")
                    return
                password = data[pos:pos+password_len]
                pos += password_len
            
            # Create client
            self.client = MQTTClient(client_id, self.transport, self)
            self.client.keep_alive = keep_alive
            self.client.clean_session = clean_session
            self.client.will_message = will_message
            
            # Register with broker
            success = await self.broker.connect_client(self.client)
            
            # Send CONNACK
            return_code = 0 if success else 2  # 0 = accepted, 2 = identifier rejected
            connack = bytearray([0x20, 0x02, 0x00, return_code])
            
            logger.debug(f"Sending CONNACK: {connack.hex()}")
            self.transport.write(connack)
            
            if success:
                logger.info(f"Client {client_id} connected successfully (protocol: {protocol_name} v{protocol_version})")
            else:
                logger.warning(f"Client {client_id} connection rejected")
                
        except Exception as e:
            logger.error(f"Error in _handle_connect: {e}")
            logger.debug(f"Exception details:", exc_info=True)
            # Send connection refused CONNACK
            try:
                connack = bytearray([0x20, 0x02, 0x00, 0x05])  # Connection refused - not authorized
                self.transport.write(connack)
            except:
                pass
    
    async def _handle_publish(self, flags: int, data: bytes):
        """Handle PUBLISH message"""
        if not self.client:
            return
            
        dup = bool(flags & 0x08)
        qos = QoS((flags >> 1) & 0x03)
        retain = bool(flags & 0x01)
        
        pos = 1 + self._get_remaining_length_size(data)
        
        # Topic name
        topic_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        topic = data[pos:pos+topic_len].decode('utf-8')
        pos += topic_len
        
        # Message ID (for QoS > 0)
        message_id = None
        if qos != QoS.AT_MOST_ONCE:
            message_id = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
        
        # Payload
        payload = data[pos:]
        
        # Create message
        message = MQTTMessage(topic, payload, qos, retain, message_id=message_id)
        
        # Send acknowledgment for QoS 1
        if qos == QoS.AT_LEAST_ONCE and message_id:
            puback = struct.pack(">BBH", 0x40, 0x02, message_id)
            self.transport.write(puback)
        
        # Publish message
        await self.broker.publish_message(message, self.client.client_id)
        
        self.client.update_last_seen()
    
    async def _handle_subscribe(self, data: bytes):
        """Handle SUBSCRIBE message"""
        if not self.client:
            return
            
        pos = 1 + self._get_remaining_length_size(data)
        
        # Message ID
        message_id = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        
        subscriptions = []
        return_codes = []
        
        while pos < len(data):
            # Topic filter
            topic_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            topic_filter = data[pos:pos+topic_len].decode('utf-8')
            pos += topic_len
            
            # QoS
            qos = QoS(data[pos] & 0x03)
            pos += 1
            
            subscriptions.append(Subscription(topic_filter, qos, self.client.client_id))
            return_codes.append(qos.value)  # Grant requested QoS
        
        # Add subscriptions
        for sub in subscriptions:
            self.client.subscriptions[sub.topic_filter] = sub.qos
            await self.broker.add_subscription(sub)
        
        # Send SUBACK
        suback = bytearray([0x90])
        suback.extend(self._encode_remaining_length(2 + len(return_codes)))
        suback.extend(struct.pack(">H", message_id))
        suback.extend(return_codes)
        self.transport.write(suback)
        
        self.client.update_last_seen()
        logger.info(f"Client {self.client.client_id} subscribed to {[s.topic_filter for s in subscriptions]}")
    
    async def _handle_unsubscribe(self, data: bytes):
        """Handle UNSUBSCRIBE message"""
        if not self.client:
            return
            
        pos = 1 + self._get_remaining_length_size(data)
        
        # Message ID
        message_id = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        
        topic_filters = []
        while pos < len(data):
            topic_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            topic_filter = data[pos:pos+topic_len].decode('utf-8')
            pos += topic_len
            topic_filters.append(topic_filter)
        
        # Remove subscriptions
        for topic_filter in topic_filters:
            if topic_filter in self.client.subscriptions:
                del self.client.subscriptions[topic_filter]
                await self.broker.remove_subscription(topic_filter, self.client.client_id)
        
        # Send UNSUBACK
        unsuback = struct.pack(">BBH", 0xB0, 0x02, message_id)
        self.transport.write(unsuback)
        
        self.client.update_last_seen()
        logger.info(f"Client {self.client.client_id} unsubscribed from {topic_filters}")
    
    async def _handle_pingreq(self):
        """Handle PINGREQ message"""
        if not self.client:
            return
            
        # Send PINGRESP
        pingresp = bytes([0xD0, 0x00])
        self.transport.write(pingresp)
        
        self.client.update_last_seen()
    
    async def _handle_disconnect(self):
        """Handle DISCONNECT message"""
        if self.client:
            logger.info(f"Client {self.client.client_id} sent DISCONNECT")
            self.client.will_message = None  # Clear will message on clean disconnect
            self.broker.disconnect_client(self.client.client_id)
    
    async def _handle_puback(self, data: bytes):
        """Handle PUBACK message"""
        if not self.client:
            return
            
        pos = 1 + self._get_remaining_length_size(data)
        message_id = struct.unpack(">H", data[pos:pos+2])[0]
        
        # Remove from pending messages
        if message_id in self.client.pending_messages:
            del self.client.pending_messages[message_id]
        
        self.client.update_last_seen()
    
    def _get_remaining_length_size(self, data: bytes) -> int:
        """Get the size of the remaining length field starting from position 1"""
        pos = 1
        size = 0
        
        while pos < len(data) and size < 4:  # Max 4 bytes for remaining length
            size += 1
            if (data[pos] & 0x80) == 0:  # If continuation bit is not set
                break
            pos += 1
        
        return size
    
    def send_message(self, message: MQTTMessage, qos: QoS):
        """Send message to client"""
        if not self.client or not self.client.connected:
            return
            
        # Build PUBLISH packet
        flags = 0
        if message.retain:
            flags |= 0x01
        flags |= (qos.value << 1)
        
        packet = bytearray([0x30 | flags])
        
        # Variable header and payload
        var_header = bytearray()
        
        # Topic name
        topic_bytes = message.topic.encode('utf-8')
        var_header.extend(struct.pack(">H", len(topic_bytes)))
        var_header.extend(topic_bytes)
        
        # Message ID (for QoS > 0)
        message_id = None
        if qos != QoS.AT_MOST_ONCE:
            message_id = self.client.get_next_message_id()
            var_header.extend(struct.pack(">H", message_id))
            # Store for potential retransmission
            self.client.pending_messages[message_id] = message
        
        # Payload
        payload = message.payload
        
        # Remaining length
        remaining_length = len(var_header) + len(payload)
        packet.extend(self._encode_remaining_length(remaining_length))
        packet.extend(var_header)
        packet.extend(payload)
        
        self.transport.write(packet)

class CertificateGenerator:
    """Generates SSL certificates for MQTT broker"""
    
    @staticmethod
    def generate_self_signed_cert(hostname: str = "localhost", 
                                 cert_file: str = "mqtt_server.pem", 
                                 key_file: str = "mqtt_server_key.pem"):
        """Generate a self-signed certificate for the MQTT broker"""
        
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        
        # Create certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MQTT Broker"),
            x509.NameAttribute(NameOID.COMMON_NAME, hostname),
        ])
        
        cert = x509.CertificateBuilder() \
            .subject_name(subject) \
            .issuer_name(issuer) \
            .public_key(private_key.public_key()) \
            .serial_number(x509.random_serial_number()) \
            .not_valid_before(datetime.datetime.utcnow()) \
            .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365)) \
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName(hostname),
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                ]),
                critical=False,
            ) \
            .sign(private_key, hashes.SHA256())
        
        # Write certificate to PEM file
        with open(cert_file, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        
        # Write private key to PEM file
        with open(key_file, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ))
        
        logger.info(f"Generated SSL certificate: {cert_file}")
        logger.info(f"Generated SSL private key: {key_file}")
        
        return cert_file, key_file

class MQTTBroker:
    """Main MQTT Broker class with SSL/TLS support"""
    
    def __init__(self, host: str = 'localhost', port: int = 1883, 
                 ssl_port: int = 8883, use_ssl: bool = True,
                 cert_file: str = None, key_file: str = None,
                 log_messages: bool = True, log_raw_data: bool = False):
        self.host = host
        self.port = port
        self.ssl_port = ssl_port
        self.use_ssl = use_ssl
        self.cert_file = cert_file or "mqtt_server.pem"
        self.key_file = key_file or "mqtt_server_key.pem"
        self.log_messages = log_messages
        self.log_raw_data = log_raw_data
        
        self.clients: Dict[str, MQTTClient] = {}
        self.subscriptions: Dict[str, Set[str]] = {}  # topic_filter -> set of client_ids
        self.retained_messages: Dict[str, MQTTMessage] = {}
        self.server: Optional[asyncio.Server] = None
        self.ssl_server: Optional[asyncio.Server] = None
        self.running = False
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_sent': 0,
            'clients_connected': 0,
            'clients_disconnected': 0,
            'ssl_connections': 0,
            'plain_connections': 0,
            'start_time': time.time()
        }
    
    async def start(self):
        """Start the MQTT broker with SSL/TLS support"""
        loop = asyncio.get_event_loop()
        
        # Generate certificates if they don't exist
        if self.use_ssl and (not os.path.exists(self.cert_file) or not os.path.exists(self.key_file)):
            logger.info("SSL certificates not found, generating self-signed certificates...")
            CertificateGenerator.generate_self_signed_cert(
                hostname=self.host,
                cert_file=self.cert_file,
                key_file=self.key_file
            )
        
        # Start plain MQTT server (port 1883)
        self.server = await loop.create_server(
            lambda: MQTTProtocol(self, use_ssl=False),
            self.host,
            self.port
        )
        logger.info(f"MQTT Broker (plain) started on {self.host}:{self.port}")
        
        # Start SSL/TLS MQTT server (port 8883)
        if self.use_ssl:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(self.cert_file, self.key_file)
            
            # Optional: Configure SSL settings for better security
            ssl_context.set_ciphers('ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS')
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            
            self.ssl_server = await loop.create_server(
                lambda: MQTTProtocol(self, use_ssl=True),
                self.host,
                self.ssl_port,
                ssl=ssl_context
            )
            logger.info(f"MQTT Broker (SSL/TLS) started on {self.host}:{self.ssl_port}")
        
        self.running = True
        
        # Start background tasks
        asyncio.create_task(self._cleanup_clients())
        asyncio.create_task(self._stats_reporter())
        
        return self.server, self.ssl_server if self.use_ssl else None
    
    async def stop(self):
        """Stop the MQTT broker"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        if self.ssl_server:
            self.ssl_server.close()
            await self.ssl_server.wait_closed()
        
        # Disconnect all clients
        for client in list(self.clients.values()):
            client.disconnect()
        
        self.running = False
        logger.info("MQTT Broker stopped")
    
    async def connect_client(self, client: MQTTClient) -> bool:
        """Connect a new client"""
        # Disconnect existing client with same ID if present
        if client.client_id in self.clients:
            old_client = self.clients[client.client_id]
            old_client.disconnect()
            logger.info(f"Disconnected existing client {client.client_id}")
        
        self.clients[client.client_id] = client
        self.stats['clients_connected'] += 1
        
        # Send retained messages for existing subscriptions
        await self._send_retained_messages(client)
        
        logger.info(f"Client {client.client_id} connected. Total clients: {len(self.clients)}")
        
        # Send HELLO message to newly connected client (compatibility feature)
        # This is done after a short delay to ensure CONNACK is processed first
        asyncio.create_task(self._send_hello_message(client))
        
        return True
    
    async def _send_hello_message(self, client: MQTTClient):
        """Send HELLO message to client after a short delay"""
        try:
            # Small delay to ensure CONNACK is sent and processed first
            await asyncio.sleep(0.1)
            
            hello_message = MQTTMessage(
                topic=f"$SYS/broker/connection/{client.client_id}",
                payload=b"HELLO",
                qos=QoS.AT_MOST_ONCE,
                retain=False
            )
            
            # Send directly to this client
            client.protocol.send_message(hello_message, QoS.AT_MOST_ONCE)
            logger.debug(f"Sent HELLO message to client {client.client_id}")
            
        except Exception as e:
            logger.warning(f"Failed to send HELLO message to client {client.client_id}: {e}")
    
    def disconnect_client(self, client_id: str):
        """Disconnect a client"""
        if client_id not in self.clients:
            return
            
        client = self.clients[client_id]
        
        # Send will message if present
        if client.will_message:
            asyncio.create_task(self.publish_message(client.will_message, client_id))
        
        # Remove subscriptions
        topics_to_remove = []
        for topic_filter in list(self.subscriptions.keys()):
            if client_id in self.subscriptions[topic_filter]:
                self.subscriptions[topic_filter].discard(client_id)
                if not self.subscriptions[topic_filter]:
                    topics_to_remove.append(topic_filter)
        
        for topic in topics_to_remove:
            del self.subscriptions[topic]
        
        # Remove client
        del self.clients[client_id]
        self.stats['clients_disconnected'] += 1
        
        logger.info(f"Client {client_id} disconnected. Total clients: {len(self.clients)}")
    
    async def add_subscription(self, subscription: Subscription):
        """Add a client subscription"""
        if subscription.topic_filter not in self.subscriptions:
            self.subscriptions[subscription.topic_filter] = set()
        
        self.subscriptions[subscription.topic_filter].add(subscription.client_id)
        
        # Send retained messages that match this subscription
        client = self.clients.get(subscription.client_id)
        if client:
            for topic, message in self.retained_messages.items():
                if TopicMatcher.matches(subscription.topic_filter, topic):
                    client.protocol.send_message(message, subscription.qos)
    
    async def remove_subscription(self, topic_filter: str, client_id: str):
        """Remove a client subscription"""
        if topic_filter in self.subscriptions:
            self.subscriptions[topic_filter].discard(client_id)
            if not self.subscriptions[topic_filter]:
                del self.subscriptions[topic_filter]
    
    async def publish_message(self, message: MQTTMessage, publisher_id: str):
        """Publish a message to all matching subscribers"""
        self.stats['messages_received'] += 1
        
        # Log processed message data in JSON format
        try:
            message_data = {
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "publisher_id": publisher_id,
                "topic": message.topic,
                "qos": message.qos.value,
                "retain": message.retain,
                "payload_size": len(message.payload),
                "payload_preview": message.payload[:100].decode('utf-8', errors='replace') if message.payload else "",
                "payload_full": None
            }
            
            # Try to decode payload as JSON
            if message.payload:
                try:
                    import json
                    decoded_payload = message.payload.decode('utf-8')
                    # Try to parse as JSON
                    json_payload = json.loads(decoded_payload)
                    message_data["payload_type"] = "json"
                    message_data["payload_json"] = json_payload
                except json.JSONDecodeError:
                    # Not JSON, treat as string
                    message_data["payload_type"] = "text"
                    message_data["payload_text"] = decoded_payload
                except UnicodeDecodeError:
                    # Binary data
                    message_data["payload_type"] = "binary"
                    message_data["payload_hex"] = message.payload.hex()
            else:
                message_data["payload_type"] = "empty"
            
            logger.info(f"📨 Message received: {json.dumps(message_data, indent=2)}")
            
        except Exception as e:
            logger.debug(f"Error formatting message data: {e}")
            # Fallback to simple logging
            payload_preview = message.payload[:50].decode('utf-8', errors='replace') if message.payload else "(empty)"
            logger.info(f"📨 Message: topic='{message.topic}', qos={message.qos.value}, "
                       f"retain={message.retain}, payload='{payload_preview}{'...' if len(message.payload) > 50 else ''}'")
        
        # Store retained message
        if message.retain:
            if message.payload:  # Non-empty payload
                self.retained_messages[message.topic] = message
            else:  # Empty payload removes retained message
                self.retained_messages.pop(message.topic, None)
        
        # Find matching subscriptions
        recipients = set()
        for topic_filter, client_ids in self.subscriptions.items():
            if TopicMatcher.matches(topic_filter, message.topic):
                recipients.update(client_ids)
        
        # Send to subscribers (except publisher for certain cases)
        sent_count = 0
        for client_id in recipients:
            if client_id == publisher_id:
                continue  # Don't send back to publisher
                
            client = self.clients.get(client_id)
            if client and client.connected:
                # Determine QoS (minimum of message QoS and subscription QoS)
                sub_qos = QoS.AT_MOST_ONCE
                for topic_filter, qos in client.subscriptions.items():
                    if TopicMatcher.matches(topic_filter, message.topic):
                        sub_qos = min(message.qos, qos, key=lambda x: x.value)
                        break
                
                client.protocol.send_message(message, sub_qos)
                sent_count += 1
        
        self.stats['messages_sent'] += sent_count
        
        if sent_count > 0:
            logger.debug(f"Published message on '{message.topic}' to {sent_count} clients")
    
    async def _send_retained_messages(self, client: MQTTClient):
        """Send retained messages to a newly connected client"""
        for topic, message in self.retained_messages.items():
            for topic_filter, qos in client.subscriptions.items():
                if TopicMatcher.matches(topic_filter, topic):
                    client.protocol.send_message(message, qos)
                    break
    
    async def _cleanup_clients(self):
        """Background task to cleanup dead clients"""
        while self.running:
            await asyncio.sleep(30)  # Check every 30 seconds
            
            dead_clients = []
            for client_id, client in self.clients.items():
                if not client.is_alive():
                    dead_clients.append(client_id)
            
            for client_id in dead_clients:
                logger.info(f"Cleaning up dead client: {client_id}")
                self.disconnect_client(client_id)
    
    async def _stats_reporter(self):
        """Background task to report statistics"""
        while self.running:
            await asyncio.sleep(300)  # Report every 5 minutes
            
            uptime = time.time() - self.stats['start_time']
            logger.info(f"Broker stats - Uptime: {uptime:.0f}s, "
                       f"Clients: {len(self.clients)}, "
                       f"Messages RX: {self.stats['messages_received']}, "
                       f"Messages TX: {self.stats['messages_sent']}, "
                       f"SSL connections: {self.stats['ssl_connections']}, "
                       f"Plain connections: {self.stats['plain_connections']}, "
                       f"Subscriptions: {sum(len(subs) for subs in self.subscriptions.values())}")
    
    def get_stats(self) -> dict:
        """Get broker statistics"""
        uptime = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'uptime_seconds': uptime,
            'active_clients': len(self.clients),
            'total_subscriptions': sum(len(subs) for subs in self.subscriptions.values()),
            'retained_messages': len(self.retained_messages),
            'ssl_enabled': self.use_ssl
        }

async def main():
    """Main function to run the broker with SSL/TLS support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MQTT Broker with SSL/TLS support')
    parser.add_argument('--host', default='localhost', help='Broker host (default: localhost)')
    parser.add_argument('--port', type=int, default=1883, help='Plain MQTT port (default: 1883)')
    parser.add_argument('--ssl-port', type=int, default=8883, help='SSL/TLS MQTT port (default: 8883)')
    parser.add_argument('--no-ssl', action='store_true', help='Disable SSL/TLS support')
    parser.add_argument('--cert', help='Path to SSL certificate file')
    parser.add_argument('--key', help='Path to SSL private key file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--log-messages', action='store_true', default=True, help='Log processed messages in JSON format (default: enabled)')
    parser.add_argument('--no-log-messages', action='store_false', dest='log_messages', help='Disable message logging')
    parser.add_argument('--log-raw-data', action='store_true', help='Enable raw data logging (very verbose)')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    use_ssl = not args.no_ssl
    broker = MQTTBroker(
        host=args.host,
        port=args.port,
        ssl_port=args.ssl_port,
        use_ssl=use_ssl,
        cert_file=args.cert,
        key_file=args.key,
        log_messages=args.log_messages,
        log_raw_data=args.log_raw_data
    )
    
    try:
        servers = await broker.start()
        
        logger.info("MQTT Broker is running...")
        logger.info("Press Ctrl+C to stop")
        
        if use_ssl:
            logger.info(f"Plain MQTT: mqtt://{args.host}:{args.port}")
            logger.info(f"SSL/TLS MQTT: mqtts://{args.host}:{args.ssl_port}")
        else:
            logger.info(f"Plain MQTT only: mqtt://{args.host}:{args.port}")
        
        if args.log_messages:
            logger.info("📨 Message logging: ENABLED (JSON format)")
        else:
            logger.info("📨 Message logging: DISABLED")
        
        if args.log_raw_data:
            logger.info("🔍 Raw data logging: ENABLED (very verbose)")
        
        # Keep the servers running
        if isinstance(servers, tuple):
            plain_server, ssl_server = servers
            async with plain_server:
                if ssl_server:
                    async with ssl_server:
                        await asyncio.gather(
                            plain_server.serve_forever(),
                            ssl_server.serve_forever()
                        )
                else:
                    await plain_server.serve_forever()
        else:
            async with servers:
                await servers.serve_forever()
            
    except KeyboardInterrupt:
        pass  # Handle Ctrl+C gracefully
    except Exception as e:
        logger.error(f"Broker error: {e}")
        raise
    finally:
        try:
            logger.info("🛑 Shutting down broker...")
            await broker.stop()
            logger.info("✅ Broker stopped cleanly")
            # Give a moment for cleanup
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error during broker shutdown: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Broker stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import sys
        sys.exit(1)
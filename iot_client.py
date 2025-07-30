#!/usr/bin/env python3
"""
Compatible IoT MQTT Client
A Python MQTT client that's fully compatible with the IoT broker
"""

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

class CompatibleMQTTClient:
    """Compatible MQTT client for the IoT broker"""
    
    def __init__(self, 
                 client_id: Optional[str] = None,
                 clean_session: bool = True,
                 keep_alive: int = 60,
                 auto_reconnect: bool = True,
                 reconnect_delay: float = 5.0,
                 max_reconnect_attempts: int = 0):  # 0 = infinite
        self.client_id = client_id or f"iot_client_{uuid.uuid4().hex[:8]}"
        self.clean_session = clean_session
        self.keep_alive = keep_alive
        self.auto_reconnect = auto_reconnect
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        
        # Connection parameters (stored for reconnection)
        self.host = "localhost"
        self.port = 1883
        self.ssl_port = 8883
        self.use_ssl = False
        self.cert_file: Optional[str] = None
        self.verify_cert = False
        
        # Message handling
        self.message_id = 1
        self.pending_acks: Dict[int, asyncio.Event] = {}
        self.subscriptions: Dict[str, QoS] = {}
        
        # Callbacks
        self.on_connect: Optional[Callable] = None
        self.on_disconnect: Optional[Callable] = None
        self.on_message: Optional[Callable] = None
        self.on_hello: Optional[Callable] = None
        self.on_reconnect: Optional[Callable] = None
        
        # Background tasks
        self._read_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        
        # Authentication
        self.username: Optional[str] = None
        self.password: Optional[str] = None
        
        # Connection received HELLO message
        self.hello_received = False
        self.hello_event = asyncio.Event()
        
        # Reconnection tracking
        self.reconnect_attempts = 0
        self.last_disconnect_time = 0
        self.manual_disconnect = False
    
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
    
    def _decode_remaining_length(self, data: bytes, start_pos: int = 1) -> Tuple[int, int]:
        """Decode MQTT remaining length field"""
        length = 0
        multiplier = 1
        pos = start_pos
        
        while pos < len(data):
            byte = data[pos]
            length += (byte & 0x7F) * multiplier
            pos += 1
            
            if (byte & 0x80) == 0:
                return length, pos - start_pos
                
            multiplier *= 128
            if multiplier > 128 * 128 * 128:
                raise ValueError("Remaining length exceeds maximum")
        
        raise ValueError("Incomplete remaining length")
    
    def _encode_string(self, s: str) -> bytes:
        """Encode a string with length prefix"""
        s_bytes = s.encode('utf-8')
        return struct.pack(">H", len(s_bytes)) + s_bytes
    
    def _decode_string(self, data: bytes, pos: int) -> Tuple[str, int]:
        """Decode a string with length prefix"""
        if pos + 2 > len(data):
            raise ValueError("Not enough data for string length")
        
        str_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        
        if pos + str_len > len(data):
            raise ValueError("Not enough data for string content")
        
        string = data[pos:pos+str_len].decode('utf-8')
        return string, pos + str_len
    
    def get_next_message_id(self) -> int:
        """Get next message ID"""
        msg_id = self.message_id
        self.message_id = (self.message_id % 65535) + 1
        return msg_id
    
    async def connect(self, host: str = "localhost", port: int = 1883, 
                     ssl_port: int = 8883, use_ssl: bool = False,
                     username: str = None, password: str = None,
                     cert_file: str = None, verify_cert: bool = False) -> bool:
        """Connect to MQTT broker"""
        if self.state != ConnectionState.DISCONNECTED:
            logger.warning("Client already connected or connecting")
            return False
        
        # Store connection parameters for reconnection
        self.host = host
        self.port = port
        self.ssl_port = ssl_port
        self.use_ssl = use_ssl
        self.cert_file = cert_file
        self.verify_cert = verify_cert
        self.username = username
        self.password = password
        
        success = await self._connect_internal()
        
        # If initial connection failed and auto_reconnect is enabled, start reconnection
        if not success and self.auto_reconnect and not self.manual_disconnect:
            logger.info("🔄 Initial connection failed, starting automatic connection attempts...")
            self._start_reconnect_task()
            # Return True to indicate that connection process has started
            return True
        
        return success
    
    async def _connect_internal(self) -> bool:
        """Internal connection method"""
        if self.state != ConnectionState.DISCONNECTED:
            return False
        
        self.state = ConnectionState.CONNECTING
        self.manual_disconnect = False
        
        target_port = self.ssl_port if self.use_ssl else self.port
        
        try:
            logger.info(f"Connecting to {self.host}:{target_port} ({'SSL' if self.use_ssl else 'Plain'})")
            
            if self.use_ssl:
                ssl_context = ssl.create_default_context()
                if self.cert_file:
                    ssl_context.load_verify_locations(self.cert_file)
                if not self.verify_cert:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, target_port, ssl=ssl_context
                )
                
                ssl_object = self.writer.get_extra_info('ssl_object')
                if ssl_object:
                    logger.info(f"SSL connection established: {ssl_object.version()}")
            else:
                self.reader, self.writer = await asyncio.open_connection(self.host, target_port)
            
            # Send CONNECT packet
            if not await self._send_connect():
                self.state = ConnectionState.DISCONNECTED
                return False
            
            # Wait for CONNACK
            connack_packet = await self._read_packet()
            if not self._handle_connack(connack_packet):
                self.state = ConnectionState.DISCONNECTED
                return False
            
            self.state = ConnectionState.CONNECTED
            self.reconnect_attempts = 0  # Reset reconnect attempts on successful connection
            
            # Start background tasks
            self._read_task = asyncio.create_task(self._read_loop())
            self._ping_task = asyncio.create_task(self._ping_loop())
            
            # Wait for HELLO message (broker compatibility feature)
            try:
                await asyncio.wait_for(self.hello_event.wait(), timeout=5.0)
                logger.info("Received HELLO message from broker")
            except asyncio.TimeoutError:
                logger.warning("No HELLO message received from broker (may not be required)")
            
            # Restore subscriptions after reconnection
            if hasattr(self, '_stored_subscriptions'):
                logger.info("Restoring subscriptions after reconnection...")
                for topic, qos in self._stored_subscriptions.items():
                    try:
                        await self.subscribe(topic, qos)
                    except Exception as e:
                        logger.error(f"Failed to restore subscription to {topic}: {e}")
            
            logger.info(f"Successfully connected as {self.client_id}")
            
            if self.on_connect:
                try:
                    await self.on_connect()
                except Exception as e:
                    logger.error(f"Error in on_connect callback: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self.state = ConnectionState.DISCONNECTED
            await self._cleanup_connection()
            
            # Don't start reconnection here if we're already in a reconnection loop
            # (this method might be called from _reconnect_loop)
            
            return False
    
    async def _send_connect(self) -> bool:
        """Send CONNECT packet"""
        try:
            # Variable header
            protocol_name = b"MQTT"
            protocol_level = 4  # MQTT 3.1.1
            
            # Connect flags
            connect_flags = 0
            if self.clean_session:
                connect_flags |= 0x02
            if self.username:
                connect_flags |= 0x80
            if self.password:
                connect_flags |= 0x40
            
            var_header = struct.pack(">H", len(protocol_name)) + protocol_name
            var_header += struct.pack(">BBH", protocol_level, connect_flags, self.keep_alive)
            
            # Payload
            payload = self._encode_string(self.client_id)
            
            if self.username:
                payload += self._encode_string(self.username)
            if self.password:
                password_bytes = self.password.encode('utf-8') if isinstance(self.password, str) else self.password
                payload += struct.pack(">H", len(password_bytes)) + password_bytes
            
            # Fixed header
            remaining_length = len(var_header) + len(payload)
            fixed_header = bytes([0x10]) + self._encode_remaining_length(remaining_length)
            
            packet = fixed_header + var_header + payload
            self.writer.write(packet)
            await self.writer.drain()
            
            logger.debug("CONNECT packet sent")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send CONNECT: {e}")
            return False
    
    def _handle_connack(self, packet: bytes) -> bool:
        """Handle CONNACK packet"""
        logger.debug(f"Received packet: {packet.hex() if packet else 'None'} (length: {len(packet) if packet else 0})")
        
        if not packet or len(packet) < 4:
            logger.error(f"Invalid CONNACK packet: too short (got {len(packet) if packet else 0} bytes, need 4)")
            return False
        
        if packet[0] != 0x20:
            logger.error(f"Expected CONNACK (0x20), got {packet[0]:02x}")
            logger.debug(f"Full packet: {packet.hex()}")
            return False
        
        session_present = bool(packet[2] & 0x01)
        return_code = packet[3]
        
        logger.debug(f"CONNACK: session_present={session_present}, return_code={return_code}")
        
        if return_code == 0:
            logger.info(f"Connection accepted (session_present: {session_present})")
            return True
        else:
            error_messages = {
                1: "Connection refused - unacceptable protocol version",
                2: "Connection refused - identifier rejected",
                3: "Connection refused - server unavailable", 
                4: "Connection refused - bad user name or password",
                5: "Connection refused - not authorized"
            }
            error_msg = error_messages.get(return_code, f"Unknown error code: {return_code}")
            logger.error(f"Authentication failed: {error_msg}")
            return False
    
    async def _read_packet(self) -> Optional[bytes]:
        """Read a complete MQTT packet"""
        try:
            # Read fixed header
            fixed_header = await self.reader.read(1)
            if not fixed_header:
                logger.debug("No data received for fixed header")
                return None
            
            logger.debug(f"Fixed header byte: 0x{fixed_header[0]:02x}")
            
            # Read remaining length
            remaining_length = 0
            multiplier = 1
            length_bytes = 0
            length_data = bytearray()
            
            while True:
                byte_data = await self.reader.read(1)
                if not byte_data:
                    logger.debug("No data received for remaining length")
                    return None
                
                byte = byte_data[0]
                length_data.append(byte)
                length_bytes += 1
                remaining_length += (byte & 0x7F) * multiplier
                
                logger.debug(f"Remaining length byte {length_bytes}: 0x{byte:02x}, current total: {remaining_length}")
                
                if (byte & 0x80) == 0:
                    break
                    
                multiplier *= 128
                if length_bytes > 4:
                    raise ValueError("Remaining length exceeds 4 bytes")
            
            logger.debug(f"Total remaining length: {remaining_length}")
            
            # Read remaining data
            if remaining_length > 0:
                remaining_data = await self.reader.read(remaining_length)
                if len(remaining_data) != remaining_length:
                    logger.debug(f"Expected {remaining_length} bytes, got {len(remaining_data)}")
                    return None
                
                full_packet = fixed_header + length_data + remaining_data
                logger.debug(f"Complete packet: {full_packet.hex()}")
                return full_packet
            else:
                full_packet = fixed_header + length_data
                logger.debug(f"Complete packet (no payload): {full_packet.hex()}")
                return full_packet
                
        except Exception as e:
            logger.error(f"Error reading packet: {e}")
            return None
    
    def _start_reconnect_task(self):
        """Start the reconnection task"""
        if self._reconnect_task and not self._reconnect_task.done():
            logger.debug("Reconnection task already running")
            return  # Reconnection already in progress
        
        if self.manual_disconnect:
            logger.debug("Manual disconnect - not starting reconnection")
            return
        
        if self.state != ConnectionState.DISCONNECTED:
            logger.debug(f"Not disconnected (state: {self.state}) - not starting reconnection")
            return
        
        logger.debug("Starting new reconnection task")
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())
    
    async def _reconnect_loop(self):
        """Background task to handle reconnection"""
        while (self.auto_reconnect and 
               not self.manual_disconnect and 
               self.state == ConnectionState.DISCONNECTED and
               (self.max_reconnect_attempts == 0 or self.reconnect_attempts < self.max_reconnect_attempts)):
            
            self.reconnect_attempts += 1
            
            # For initial connection attempts (first few tries), use shorter delays
            if self.reconnect_attempts <= 3:
                wait_time = min(self.reconnect_delay, 5.0)  # Max 5 seconds for initial attempts
            else:
                wait_time = min(self.reconnect_delay * (2 ** min(self.reconnect_attempts - 4, 5)), 60)  # Exponential backoff, max 60s
            
            if self.reconnect_attempts == 1:
                logger.info(f"Connecting to broker... (attempt {self.reconnect_attempts})")
            else:
                logger.info(f"Reconnection attempt {self.reconnect_attempts} in {wait_time:.1f} seconds...")
            
            try:
                if self.reconnect_attempts > 1:  # Skip delay for first attempt
                    await asyncio.sleep(wait_time)
                
                if self.manual_disconnect:  # Check if manually disconnected during wait
                    logger.info("Manual disconnect detected, stopping reconnection")
                    break
                
                if self.state != ConnectionState.DISCONNECTED:  # Already connected
                    logger.info("Already connected, stopping reconnection loop")
                    break
                
                if self.reconnect_attempts == 1:
                    logger.debug(f"Attempting initial connection to {self.host}:{self.ssl_port if self.use_ssl else self.port}...")
                else:
                    logger.info(f"Attempting to reconnect to {self.host}:{self.ssl_port if self.use_ssl else self.port}...")
                
                # Clear any existing connection state before reconnecting
                self.hello_received = False
                self.hello_event.clear()
                
                success = await self._connect_internal()
                if success:
                    if self.reconnect_attempts == 1:
                        logger.info("✅ Initial connection successful!")
                    else:
                        logger.info("✅ Reconnection successful!")
                    
                    if self.on_reconnect and self.reconnect_attempts > 1:
                        try:
                            await self.on_reconnect()
                        except Exception as e:
                            logger.error(f"Error in on_reconnect callback: {e}")
                    break
                else:
                    if self.reconnect_attempts <= 3:
                        logger.debug(f"❌ Connection attempt {self.reconnect_attempts} failed")
                    else:
                        logger.warning(f"❌ Reconnection attempt {self.reconnect_attempts} failed")
                    
            except asyncio.CancelledError:
                logger.info("Connection attempts cancelled")
                break
            except Exception as e:
                logger.error(f"Error during connection attempt {self.reconnect_attempts}: {e}")
        
        if (self.max_reconnect_attempts > 0 and 
            self.reconnect_attempts >= self.max_reconnect_attempts and
            self.state == ConnectionState.DISCONNECTED):
            logger.error(f"❌ Max connection attempts ({self.max_reconnect_attempts}) reached. Giving up.")
        
        # Clear the reconnect task reference
        self._reconnect_task = None
    
    async def _read_loop(self):
        """Background task to read incoming packets"""
        while self.state == ConnectionState.CONNECTED:
            try:
                packet = await self._read_packet()
                if packet is None:
                    logger.warning("Connection lost - no data received")
                    self.last_disconnect_time = time.time()
                    break
                
                await self._handle_packet(packet)
                
            except Exception as e:
                logger.error(f"Error in read loop: {e}")
                self.last_disconnect_time = time.time()
                break
        
        # Connection lost
        if self.state == ConnectionState.CONNECTED:
            self.state = ConnectionState.DISCONNECTED
            logger.warning("🔴 Connection lost")
            
            # Store current subscriptions for restoration
            if self.subscriptions:
                self._stored_subscriptions = self.subscriptions.copy()
            
            if self.on_disconnect:
                try:
                    await self.on_disconnect()
                except Exception as e:
                    logger.error(f"Error in on_disconnect callback: {e}")
            
            # Start reconnection if enabled and not manually disconnected
            if self.auto_reconnect and not self.manual_disconnect:
                logger.info("🔄 Starting automatic reconnection...")
                self._start_reconnect_task()
    
    async def _handle_packet(self, packet: bytes):
        """Handle incoming MQTT packet"""
        if not packet:
            return
        
        msg_type = (packet[0] >> 4) & 0x0F
        
        if msg_type == 3:  # PUBLISH
            await self._handle_publish(packet)
        elif msg_type == 4:  # PUBACK
            await self._handle_puback(packet)
        elif msg_type == 9:  # SUBACK
            await self._handle_suback(packet)
        elif msg_type == 11:  # UNSUBACK
            await self._handle_unsuback(packet)
        elif msg_type == 13:  # PINGRESP
            logger.debug("Received PINGRESP")
        else:
            logger.debug(f"Received packet type: {msg_type}")
    
    async def _handle_publish(self, packet: bytes):
        """Handle PUBLISH packet"""
        try:
            flags = packet[0] & 0x0F
            qos = QoS((flags >> 1) & 0x03)
            retain = bool(flags & 0x01)
            
            # Parse remaining length and skip to variable header
            remaining_length, length_size = self._decode_remaining_length(packet, 1)
            pos = 1 + length_size
            
            # Parse topic
            topic, pos = self._decode_string(packet, pos)
            
            # Parse message ID for QoS > 0
            message_id = None
            if qos != QoS.AT_MOST_ONCE:
                if pos + 2 > len(packet):
                    logger.error("PUBLISH packet missing message ID")
                    return
                message_id = struct.unpack(">H", packet[pos:pos+2])[0]
                pos += 2
            
            # Extract payload
            payload = packet[pos:]
            
            # Send PUBACK for QoS 1
            if qos == QoS.AT_LEAST_ONCE and message_id:
                puback = struct.pack(">BBH", 0x40, 0x02, message_id)
                self.writer.write(puback)
                await self.writer.drain()
            
            # Check for HELLO message
            if topic.startswith("$SYS/broker/connection/") and payload == b"HELLO":
                self.hello_received = True
                self.hello_event.set()
                logger.debug("Received HELLO message from broker")
                
                if self.on_hello:
                    try:
                        await self.on_hello(topic, payload)
                    except Exception as e:
                        logger.error(f"Error in on_hello callback: {e}")
                return
            
            # Regular message handling
            logger.info(f"Received message on '{topic}': {payload.decode('utf-8', errors='ignore')}")
            
            if self.on_message:
                try:
                    await self.on_message(topic, payload, qos, retain)
                except Exception as e:
                    logger.error(f"Error in on_message callback: {e}")
                    
        except Exception as e:
            logger.error(f"Error handling PUBLISH: {e}")
    
    async def _handle_puback(self, packet: bytes):
        """Handle PUBACK packet"""
        try:
            if len(packet) >= 4:
                message_id = struct.unpack(">H", packet[2:4])[0]
                if message_id in self.pending_acks:
                    self.pending_acks[message_id].set()
                    logger.debug(f"Received PUBACK for message {message_id}")
        except Exception as e:
            logger.error(f"Error handling PUBACK: {e}")
    
    async def _handle_suback(self, packet: bytes):
        """Handle SUBACK packet"""
        try:
            if len(packet) >= 5:
                message_id = struct.unpack(">H", packet[2:4])[0]
                return_codes = list(packet[4:])
                logger.info(f"Subscription confirmed (message_id: {message_id}, return_codes: {return_codes})")
                
                if message_id in self.pending_acks:
                    self.pending_acks[message_id].set()
        except Exception as e:
            logger.error(f"Error handling SUBACK: {e}")
    
    async def _handle_unsuback(self, packet: bytes):
        """Handle UNSUBACK packet"""
        try:
            if len(packet) >= 4:
                message_id = struct.unpack(">H", packet[2:4])[0]
                logger.info(f"Unsubscription confirmed (message_id: {message_id})")
                
                if message_id in self.pending_acks:
                    self.pending_acks[message_id].set()
        except Exception as e:
            logger.error(f"Error handling UNSUBACK: {e}")
    
    async def _ping_loop(self):
        """Background task to send PINGREQ packets"""
        if self.keep_alive == 0:
            return
        
        interval = self.keep_alive * 0.75  # Send ping at 75% of keep_alive interval
        
        while self.state == ConnectionState.CONNECTED:
            try:
                await asyncio.sleep(interval)
                
                if self.state == ConnectionState.CONNECTED:
                    # Send PINGREQ
                    pingreq = bytes([0xC0, 0x00])
                    self.writer.write(pingreq)
                    await self.writer.drain()
                    logger.debug("Sent PINGREQ")
                    
            except Exception as e:
                logger.error(f"Error in ping loop: {e}")
                break
    
    async def subscribe(self, topic: str, qos: QoS = QoS.AT_MOST_ONCE, timeout: float = 10.0) -> bool:
        """Subscribe to a topic"""
        if self.state != ConnectionState.CONNECTED:
            logger.error("Not connected to broker")
            return False
        
        try:
            message_id = self.get_next_message_id()
            
            # Create SUBSCRIBE packet
            var_header = struct.pack(">H", message_id)
            payload = self._encode_string(topic) + bytes([qos.value])
            
            remaining_length = len(var_header) + len(payload)
            fixed_header = bytes([0x82]) + self._encode_remaining_length(remaining_length)
            
            packet = fixed_header + var_header + payload
            
            # Set up acknowledgment waiting
            ack_event = asyncio.Event()
            self.pending_acks[message_id] = ack_event
            
            # Send packet
            self.writer.write(packet)
            await self.writer.drain()
            
            # Wait for SUBACK
            try:
                await asyncio.wait_for(ack_event.wait(), timeout=timeout)
                self.subscriptions[topic] = qos
                logger.info(f"Successfully subscribed to '{topic}' with QoS {qos.value}")
                return True
            except asyncio.TimeoutError:
                logger.error(f"Subscription to '{topic}' timed out")
                return False
            finally:
                self.pending_acks.pop(message_id, None)
                
        except Exception as e:
            logger.error(f"Failed to subscribe to '{topic}': {e}")
            # If this is a connection error, trigger reconnection
            if "not connected" in str(e).lower() or "connection" in str(e).lower():
                if self.auto_reconnect and not self.manual_disconnect:
                    self.state = ConnectionState.DISCONNECTED
                    self._start_reconnect_task()
            return False
    
    async def unsubscribe(self, topic: str, timeout: float = 10.0) -> bool:
        """Unsubscribe from a topic"""
        if self.state != ConnectionState.CONNECTED:
            logger.error("Not connected to broker")
            return False
        
        try:
            message_id = self.get_next_message_id()
            
            # Create UNSUBSCRIBE packet
            var_header = struct.pack(">H", message_id)
            payload = self._encode_string(topic)
            
            remaining_length = len(var_header) + len(payload)
            fixed_header = bytes([0xA2]) + self._encode_remaining_length(remaining_length)
            
            packet = fixed_header + var_header + payload
            
            # Set up acknowledgment waiting
            ack_event = asyncio.Event()
            self.pending_acks[message_id] = ack_event
            
            # Send packet
            self.writer.write(packet)
            await self.writer.drain()
            
            # Wait for UNSUBACK
            try:
                await asyncio.wait_for(ack_event.wait(), timeout=timeout)
                self.subscriptions.pop(topic, None)
                logger.info(f"Successfully unsubscribed from '{topic}'")
                return True
            except asyncio.TimeoutError:
                logger.error(f"Unsubscription from '{topic}' timed out")
                return False
            finally:
                self.pending_acks.pop(message_id, None)
                
        except Exception as e:
            logger.error(f"Failed to unsubscribe from '{topic}': {e}")
            return False
    
    async def publish(self, topic: str, payload: bytes, qos: QoS = QoS.AT_MOST_ONCE, 
                     retain: bool = False, timeout: float = 10.0) -> bool:
        """Publish a message"""
        if self.state != ConnectionState.CONNECTED:
            logger.error("Not connected to broker")
            return False
        
        try:
            # Fixed header flags
            flags = 0
            if retain:
                flags |= 0x01
            flags |= (qos.value << 1)
            
            # Variable header
            var_header = self._encode_string(topic)
            message_id = None
            
            if qos != QoS.AT_MOST_ONCE:
                message_id = self.get_next_message_id()
                var_header += struct.pack(">H", message_id)
            
            # Fixed header
            remaining_length = len(var_header) + len(payload)
            fixed_header = bytes([0x30 | flags]) + self._encode_remaining_length(remaining_length)
            
            packet = fixed_header + var_header + payload
            
            # Set up acknowledgment waiting for QoS > 0
            ack_event = None
            if qos == QoS.AT_LEAST_ONCE and message_id:
                ack_event = asyncio.Event()
                self.pending_acks[message_id] = ack_event
            
            # Send packet
            self.writer.write(packet)
            await self.writer.drain()
            
            # Wait for acknowledgment if needed
            if ack_event:
                try:
                    await asyncio.wait_for(ack_event.wait(), timeout=timeout)
                    logger.info(f"Published message to '{topic}' (QoS {qos.value})")
                    return True
                except asyncio.TimeoutError:
                    logger.error(f"Publish to '{topic}' timed out")
                    return False
                finally:
                    self.pending_acks.pop(message_id, None)
            else:
                logger.info(f"Published message to '{topic}' (QoS 0)")
                return True
                
        except Exception as e:
            logger.error(f"Failed to publish to '{topic}': {e}")
            # If this is a connection error, trigger reconnection
            if "not connected" in str(e).lower() or "connection" in str(e).lower():
                if self.auto_reconnect and not self.manual_disconnect:
                    self.state = ConnectionState.DISCONNECTED
                    self._start_reconnect_task()
            return False
    
    async def disconnect(self):
        """Disconnect from broker"""
        if self.state != ConnectionState.CONNECTED:
            return
        
        self.manual_disconnect = True  # Prevent automatic reconnection
        self.state = ConnectionState.DISCONNECTING
        
        try:
            # Send DISCONNECT packet
            disconnect_packet = bytes([0xE0, 0x00])
            self.writer.write(disconnect_packet)
            await self.writer.drain()
            logger.info("Sent DISCONNECT packet")
            
        except Exception as e:
            logger.error(f"Error sending DISCONNECT: {e}")
        
        await self._cleanup_connection()
        self.state = ConnectionState.DISCONNECTED
        
        if self.on_disconnect:
            try:
                await self.on_disconnect()
            except Exception as e:
                logger.error(f"Error in on_disconnect callback: {e}")
    
    async def _cleanup_connection(self):
        """Clean up connection resources"""
        # Collect tasks to cancel
        tasks_to_cancel = []
        
        if self._read_task and not self._read_task.done():
            tasks_to_cancel.append(("read_task", self._read_task))
        
        if self._ping_task and not self._ping_task.done():
            tasks_to_cancel.append(("ping_task", self._ping_task))
        
        if self._reconnect_task and not self._reconnect_task.done():
            tasks_to_cancel.append(("reconnect_task", self._reconnect_task))
        
        # Cancel tasks one by one with proper error handling
        for task_name, task in tasks_to_cancel:
            try:
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass  # Expected when cancelling
                    except Exception as e:
                        logger.debug(f"Error cancelling {task_name}: {e}")
            except Exception as e:
                logger.debug(f"Error during {task_name} cleanup: {e}")
        
        # Close connection
        if self.writer and not self.writer.is_closing():
            try:
                self.writer.close()
                await asyncio.wait_for(self.writer.wait_closed(), timeout=5.0)
            except Exception as e:
                logger.debug(f"Error closing connection: {e}")
        
        # Clear state
        self.reader = None
        self.writer = None
        self._read_task = None
        self._ping_task = None
        # Don't clear reconnect_task here as it might be running
        self.pending_acks.clear()
        self.hello_received = False
        self.hello_event.clear()
    
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self.state == ConnectionState.CONNECTED
    
    def enable_auto_reconnect(self, enabled: bool = True, delay: float = 5.0, max_attempts: int = 0):
        """Enable or disable automatic reconnection"""
        self.auto_reconnect = enabled
        self.reconnect_delay = delay
        self.max_reconnect_attempts = max_attempts
        logger.info(f"Auto-reconnect {'enabled' if enabled else 'disabled'} "
                   f"(delay: {delay}s, max attempts: {'unlimited' if max_attempts == 0 else max_attempts})")
    
    async def wait_for_connection(self, timeout: float = 30.0) -> bool:
        """Wait for connection to be established"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.state == ConnectionState.CONNECTED:
                return True
            await asyncio.sleep(0.1)
        return False

# Example usage and test functions
async def example_client():
    """Example usage of the compatible MQTT client"""
    
    # Create client
    client = CompatibleMQTTClient(client_id="example_client")
    
    # Set up callbacks
    async def on_connect():
        logger.info("🟢 Connected to broker!")
    
    async def on_disconnect():
        logger.info("🔴 Disconnected from broker!")
    
    async def on_message(topic: str, payload: bytes, qos: QoS, retain: bool):
        message = payload.decode('utf-8', errors='ignore')
        logger.info(f"📨 Message received on '{topic}': {message}")
    
    async def on_hello(topic: str, payload: bytes):
        logger.info("👋 Received HELLO from broker - authentication successful!")
    
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_hello = on_hello
    
    try:
        # Connect to broker
        success = await client.connect(
            host="localhost",
            port=1883,
            use_ssl=False  # Set to True for SSL
        )
        
        if not success:
            logger.error("Failed to connect to broker")
            return
        
        # Subscribe to topics
        await client.subscribe("sensors/temperature", QoS.AT_LEAST_ONCE)
        await client.subscribe("sensors/humidity", QoS.AT_MOST_ONCE)
        
        # Publish some test messages
        await client.publish("sensors/temperature", b"23.5", QoS.AT_LEAST_ONCE)
        await client.publish("sensors/humidity", b"65.2", QoS.AT_MOST_ONCE)
        await client.publish("status/client", b"online", retain=True)
        
        # Listen for messages
        logger.info("Listening for messages for 30 seconds...")
        await asyncio.sleep(30)
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        await client.disconnect()

async def interactive_client():
    """Interactive MQTT client for testing"""
    client = CompatibleMQTTClient()
    
    # Callbacks
    async def on_connect():
        print("✅ Connected to broker!")
    
    async def on_disconnect():
        print("❌ Disconnected from broker!")
    
    async def on_message(topic: str, payload: bytes, qos: QoS, retain: bool):
        message = payload.decode('utf-8', errors='ignore')
        print(f"📨 [{topic}] {message} (QoS: {qos.value}, Retain: {retain})")
    
    async def on_hello(topic: str, payload: bytes):
        print("👋 HELLO received from broker - authentication successful!")
    
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_hello = on_hello
    
    print("🤖 Interactive MQTT Client")
    print("=" * 40)
    
    # Get connection details
    host = input("Broker host (localhost): ").strip() or "localhost"
    port = int(input("Port (1883): ").strip() or "1883")
    use_ssl = input("Use SSL? (y/n): ").strip().lower() == 'y'
    
    if use_ssl:
        ssl_port = int(input("SSL port (8883): ").strip() or "8883")
        cert_file = input("Certificate file (optional): ").strip() or None
        port = ssl_port
    
    # Connect
    print(f"\nConnecting to {host}:{port}...")
    success = await client.connect(
        host=host,
        port=port,
        use_ssl=use_ssl,
        cert_file=cert_file if use_ssl else None,
        verify_cert=False
    )
    
    if not success:
        print("❌ Failed to connect")
        return
    
    print("\n📋 Commands:")
    print("  sub <topic> [qos]     - Subscribe to topic")
    print("  pub <topic> <message> - Publish message")
    print("  unsub <topic>         - Unsubscribe from topic")
    print("  list                  - List subscriptions")
    print("  quit                  - Disconnect and exit")
    
    try:
        while client.state == ConnectionState.CONNECTED:
            try:
                cmd = input("\n> ").strip().split()
                if not cmd:
                    continue
                
                if cmd[0] == "sub":
                    if len(cmd) < 2:
                        print("Usage: sub <topic> [qos]")
                        continue
                    
                    topic = cmd[1]
                    qos = QoS(int(cmd[2])) if len(cmd) > 2 else QoS.AT_MOST_ONCE
                    
                    success = await client.subscribe(topic, qos)
                    if success:
                        print(f"✅ Subscribed to '{topic}' with QoS {qos.value}")
                    else:
                        print(f"❌ Failed to subscribe to '{topic}'")
                
                elif cmd[0] == "pub":
                    if len(cmd) < 3:
                        print("Usage: pub <topic> <message>")
                        continue
                    
                    topic = cmd[1]
                    message = " ".join(cmd[2:])
                    
                    success = await client.publish(topic, message.encode('utf-8'), QoS.AT_LEAST_ONCE)
                    if success:
                        print(f"✅ Published to '{topic}': {message}")
                    else:
                        print(f"❌ Failed to publish to '{topic}'")
                
                elif cmd[0] == "unsub":
                    if len(cmd) < 2:
                        print("Usage: unsub <topic>")
                        continue
                    
                    topic = cmd[1]
                    success = await client.unsubscribe(topic)
                    if success:
                        print(f"✅ Unsubscribed from '{topic}'")
                    else:
                        print(f"❌ Failed to unsubscribe from '{topic}'")
                
                elif cmd[0] == "list":
                    if client.subscriptions:
                        print("📝 Current subscriptions:")
                        for topic, qos in client.subscriptions.items():
                            print(f"  {topic} (QoS {qos.value})")
                    else:
                        print("📝 No active subscriptions")
                
                elif cmd[0] == "quit":
                    break
                
                else:
                    print("❌ Unknown command")
                    
            except EOFError:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    finally:
        print("\nDisconnecting...")
        await client.disconnect()
        print("👋 Goodbye!")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Compatible IoT MQTT Client')
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Run in interactive mode')
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
    
    # Publishing/subscribing options
    parser.add_argument('--subscribe', '-s', 
                       help='Subscribe to topic and listen')
    parser.add_argument('--publish', '-p', nargs=2, metavar=('TOPIC', 'MESSAGE'),
                       help='Publish message to topic')
    parser.add_argument('--qos', type=int, choices=[0, 1, 2], default=0,
                       help='QoS level (default: 0)')
    parser.add_argument('--retain', action='store_true',
                       help='Set retain flag for published messages')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.interactive:
            asyncio.run(interactive_client())
        elif args.subscribe:
            asyncio.run(subscribe_mode(args))
        elif args.publish:
            asyncio.run(publish_mode(args))
        else:
            asyncio.run(example_client())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Error: {e}")
        import sys
        sys.exit(1)

async def subscribe_mode(args):
    """Subscribe mode - listen to a topic"""
    client = CompatibleMQTTClient(client_id=args.client_id)
    
    async def on_message(topic: str, payload: bytes, qos: QoS, retain: bool):
        message = payload.decode('utf-8', errors='ignore')
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {topic}: {message}")
    
    async def on_hello(topic: str, payload: bytes):
        print("✅ Authentication successful (HELLO received)")
    
    client.on_message = on_message
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
    
    # Subscribe
    qos = QoS(args.qos)
    success = await client.subscribe(args.subscribe, qos)
    if not success:
        print(f"❌ Failed to subscribe to '{args.subscribe}'")
        await client.disconnect()
        return
    
    print(f"✅ Subscribed to '{args.subscribe}' with QoS {args.qos}")
    print("Listening for messages... (Press Ctrl+C to stop)")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        await client.disconnect()

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
    topic, message = args.publish
    qos = QoS(args.qos)
    
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

async def sensor_simulation():
    """Simulate IoT sensor data publishing"""
    import random
    
    client = CompatibleMQTTClient(client_id="sensor_simulator", auto_reconnect=True, reconnect_delay=5.0)
    
    async def on_connect():
        print("✅ Sensor connected to broker successfully!")
    
    async def on_disconnect():
        print("🔴 Sensor disconnected from broker")
    
    async def on_reconnect():
        print("🔄 Sensor reconnected to broker!")
    
    async def on_hello(topic: str, payload: bytes):
        print("✅ Sensor authenticated with broker!")
    
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_reconnect = on_reconnect
    client.on_hello = on_hello
    
    print("🌡️  IoT Sensor Simulator")
    print("=" * 30)
    
    try:
        # Connect to broker (will keep trying if broker isn't available)
        print("🔌 Connecting to broker...")
        success = await client.connect(host="localhost", port=1883)
        
        if not success and not client.auto_reconnect:
            print("❌ Failed to connect to broker and auto-reconnect is disabled")
            return
        elif not success:
            print("⏳ Broker not available yet, will keep trying to connect...")
        
        print("📡 Sensor will publish data every 5 seconds once connected")
        print("💡 The sensor will automatically connect when broker becomes available")
        print("Press Ctrl+C to stop")
        
        # Wait for initial connection if not already connected
        if not client.is_connected():
            print("⏳ Waiting for connection to broker...")
            connected = await client.wait_for_connection(timeout=30.0)
            if not connected:
                print("⚠️  Still waiting for broker... (will continue trying in background)")
        
        while True:
            try:
                # Only publish if connected
                if client.is_connected():
                    try:
                        # Generate random sensor data
                        temperature = round(random.uniform(18.0, 35.0), 1)
                        humidity = round(random.uniform(30.0, 80.0), 1)
                        pressure = round(random.uniform(980.0, 1020.0), 1)
                        
                        # Publish sensor readings
                        success1 = await client.publish("sensors/temperature", f"{temperature}".encode(), QoS.AT_LEAST_ONCE)
                        success2 = await client.publish("sensors/humidity", f"{humidity}".encode(), QoS.AT_LEAST_ONCE)
                        success3 = await client.publish("sensors/pressure", f"{pressure}".encode(), QoS.AT_LEAST_ONCE)
                        
                        # Send status update
                        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                        status = {"timestamp": timestamp, "temp": temperature, "humidity": humidity, "pressure": pressure}
                        success4 = await client.publish("sensors/status", json.dumps(status).encode(), retain=True)
                        
                        if all([success1, success2, success3, success4]):
                            print(f"📊 T:{temperature}°C H:{humidity}% P:{pressure}hPa")
                        else:
                            print(f"⚠️  T:{temperature}°C H:{humidity}% P:{pressure}hPa (some messages failed)")
                            
                    except Exception as e:
                        print(f"❌ Error publishing sensor data: {e}")
                else:
                    print("⏳ Waiting for broker connection...")
                    # Wait for connection to be restored
                    connected = await client.wait_for_connection(timeout=10.0)
                    if not connected:
                        print("🔄 Still waiting for broker...")
                
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                # Handle cancellation gracefully
                break
            except Exception as e:
                print(f"❌ Unexpected error in sensor loop: {e}")
                await asyncio.sleep(1)
                
    except KeyboardInterrupt:
        pass  # Handle Ctrl+C gracefully
    except Exception as e:
        print(f"❌ Fatal error in sensor simulation: {e}")
    finally:
        print("\n🛑 Stopping sensor simulation...")
        try:
            await client.disconnect()
            print("✅ Sensor disconnected cleanly")
        except Exception as e:
            print(f"⚠️  Error during disconnect: {e}")
        
        # Give a moment for cleanup
        await asyncio.sleep(0.5)

async def dashboard_client():
    """Simple dashboard client to monitor sensor data"""
    client = CompatibleMQTTClient(client_id="dashboard")
    
    sensor_data = {}
    
    async def on_connect():
        print("📊 Dashboard connected to broker!")
    
    async def on_message(topic: str, payload: bytes, qos: QoS, retain: bool):
        try:
            if topic.startswith("sensors/"):
                sensor_type = topic.split("/")[1]
                value = payload.decode('utf-8')
                
                if sensor_type == "status":
                    data = json.loads(value)
                    print(f"\n📈 Sensor Status Update:")
                    print(f"   Time: {data['timestamp']}")
                    print(f"   Temperature: {data['temp']}°C")
                    print(f"   Humidity: {data['humidity']}%")
                    print(f"   Pressure: {data['pressure']}hPa")
                else:
                    sensor_data[sensor_type] = value
                    print(f"🔔 {sensor_type.title()}: {value}")
                    
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    async def on_hello(topic: str, payload: bytes):
        print("✅ Dashboard authenticated successfully!")
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_hello = on_hello
    
    print("📊 IoT Dashboard Client")
    print("=" * 25)
    
    # Connect
    success = await client.connect(host="localhost", port=1883)
    if not success:
        print("❌ Failed to connect to broker")
        return
    
    # Subscribe to all sensor topics
    await client.subscribe("sensors/+", QoS.AT_LEAST_ONCE)
    
    print("👀 Monitoring sensor data...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping dashboard...")
    finally:
        await client.disconnect()
        print("✅ Dashboard disconnected")

# Additional utility functions for testing
async def stress_test():
    """Stress test the broker with multiple clients"""
    import random
    
    print("⚡ MQTT Broker Stress Test")
    print("=" * 30)
    
    num_clients = int(input("Number of clients (10): ").strip() or "10")
    duration = int(input("Test duration in seconds (60): ").strip() or "60")
    
    clients = []
    tasks = []
    
    async def client_worker(client_id: int):
        client = CompatibleMQTTClient(client_id=f"stress_client_{client_id}")
        clients.append(client)
        
        try:
            # Connect
            success = await client.connect(host="localhost", port=1883)
            if not success:
                return
            
            # Subscribe to test topic
            await client.subscribe(f"test/client_{client_id}", QoS.AT_MOST_ONCE)
            
            # Publish messages periodically
            for i in range(duration):
                message = f"Message {i} from client {client_id}"
                await client.publish(f"test/broadcast", message.encode(), QoS.AT_MOST_ONCE)
                
                # Random delay between 0.5-2 seconds
                await asyncio.sleep(random.uniform(0.5, 2.0))
                
        except Exception as e:
            print(f"❌ Client {client_id} error: {e}")
        finally:
            await client.disconnect()
    
    print(f"🚀 Starting {num_clients} clients for {duration} seconds...")
    
    # Start all clients
    for i in range(num_clients):
        task = asyncio.create_task(client_worker(i))
        tasks.append(task)
    
    # Wait for completion
    try:
        await asyncio.gather(*tasks)
        print("✅ Stress test completed successfully!")
    except KeyboardInterrupt:
        print("\n🛑 Stress test interrupted")
        # Cancel all tasks
        for task in tasks:
            task.cancel()
        # Wait for cleanup
        await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"📊 Test completed with {num_clients} clients")

if __name__ == "__main__":
    # Check if special modes are requested
    import sys
    
    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "sensor":
                asyncio.run(sensor_simulation())
            elif sys.argv[1] == "dashboard":
                asyncio.run(dashboard_client())
            elif sys.argv[1] == "stress":
                asyncio.run(stress_test())
            else:
                main()
        else:
            main()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
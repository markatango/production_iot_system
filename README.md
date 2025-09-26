## IoT MQTT Broker 

### Key Features
* MQTT Protocol Compliance: Full MQTT 3.1.1 protocol implementation
* Support for QoS levels 0, 1, and 2
* Topic wildcards (+ for single level, # for multi-level)
* Retained messages
* Last Will and Testament (LWT)
* Keep-alive mechanism with client cleanup

### Client Management:
* Connection handling with CONNECT/CONNACK
* Client authentication support
* Automatic client ID generation
* Session management (clean/persistent sessions)
* Graceful client disconnection

### Message Publishing & Subscribing:
* PUBLISH/PUBACK message flow
* SUBSCRIBE/SUBACK with multiple topic filters
* UNSUBSCRIBE/UNSUBACK
* Message routing to matching subscribers
* Retained message delivery to new subscribers

### Broker Features:
* Multi-client support
* Topic-based message routing
* Statistics tracking
* Background client cleanup
* Periodic stats reporting
* Proper connection lifecycle management

### Usage
Start the broker:

bash
`python mqtt_broker.py`

Connect clients: The broker runs on localhost:1883 and is compatible with any MQTT client such as:
* Paho MQTT clients
* MQTT.js
* mosquitto_pub/mosquitto_sub
* IoT device SDKs


### Test with mosquitto tools:

#### Subscribe to a topic
`mosquitto_sub -h localhost -t "sensors/temperature"`

#### Publish a message
`mosquitto_pub -h localhost -t "sensors/temperature" -m "23.5"`

### Architecture
* MQTTBroker: Main broker class managing clients and message routing
* MQTTClient: Represents connected clients with subscriptions and state
* MQTTProtocol: Handles MQTT protocol parsing and message processing
* TopicMatcher: Implements MQTT topic matching with wildcard support
* Message routing: Efficient topic-based message distribution
* The broker maintains full compatibility with EMQX clients while providing robust error handling, logging, and statistics. It supports all standard MQTT features needed for IoT applications.




## SSL Certificate Generator for MQTT Broker

### SSL/TLS Features 

* Dual Port Support: Plain MQTT (1883) and SSL/TLS MQTT (8883)
* Automatic Certificate Generation: Creates self-signed certificates if not found
* SSL Context Configuration: Secure cipher suites and TLS 1.2+ enforcement
* Connection Statistics: Tracks SSL vs plain connections
* Command Line Arguments: Flexible configuration options
* Certificate Generator (cert_generator.py)
* Self-Signed Certificate Creation: Generates PEM format certificates
* Subject Alternative Names: Supports multiple hostnames and IP addresses
* Certificate Information Display: View certificate details and validity
* Security Best Practices: Proper key usage extensions and file permissions
* SSL Test Client (mqtt_ssl_test.py)
* Dual Protocol Support: Tests both plain and SSL connections
* Interactive Mode: Manual testing capabilities
* Cross-Communication Testing: Verifies plain ↔ SSL client communication
* Certificate Verification: Optional certificate validation

### Usage Instructions
1. Generate SSL Certificates
bash
#### Generate certificates for localhost
`python cert_generator.py`

#### Generate for specific hostname
`python cert_generator.py --hostname mqtt.example.com`

#### View certificate info
`python cert_generator.py --info mqtt_server.pem`

2. Start the Broker
bash
#### Start with SSL support (default)
`python mqtt_broker.py`

#### Start with custom certificate
`python mqtt_broker.py --cert my_cert.pem --key my_key.pem`

#### Start without SSL (plain only)
`python mqtt_broker.py --no-ssl`

#### Start with verbose logging
`python mqtt_broker.py --verbose`
3. Test SSL Functionality
bash
# Run automated tests
`python mqtt_ssl_test.py`

# Interactive testing mode
`python mqtt_ssl_test.py --interactive`

# Test with custom certificate
`python mqtt_ssl_test.py --cert mqtt_server.pem`

4. Connect with Standard Clients
Plain MQTT (port 1883):

bash
`mosquitto_pub -h localhost -p 1883 -t "sensors/temp" -m "23.5"`
`mosquitto_sub -h localhost -p 1883 -t "sensors/temp"`

SSL/TLS MQTT (port 8883):

bash
# With certificate verification
`mosquitto_pub -h localhost -p 8883 --cafile mqtt_server.pem -t "sensors/temp" -m "23.5"`

# Without certificate verification (self-signed)
`mosquitto_pub -h localhost -p 8883 --insecure -t "sensors/temp" -m "23.5"`
### Key SSL/TLS Features
Automatic Certificate Generation: Creates certificates on first run
Secure Configuration: Uses strong cipher suites and TLS 1.2+
Certificate Validation: Optional client certificate verification
Mixed Mode Operation: Plain and SSL clients can communicate through the broker
Connection Monitoring: Separate statistics for SSL vs plain connections
Flexible Deployment: Works with self-signed or CA-signed certificates

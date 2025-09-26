#!/usr/bin/env python3
"""
SSL Certificate Generator for MQTT Broker
Generates self-signed certificates in PEM format for MQTT over SSL/TLS
"""

import argparse
import datetime
import ipaddress
import os
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

def generate_certificate(hostname="localhost", 
                        cert_file="mqtt_server.pem", 
                        key_file="mqtt_server_key.pem",
                        days_valid=365,
                        key_size=2048):
    """
    Generate a self-signed SSL certificate for MQTT broker
    
    Args:
        hostname: Hostname for the certificate (default: localhost)
        cert_file: Output certificate file (default: mqtt_server.pem)
        key_file: Output private key file (default: mqtt_server_key.pem)
        days_valid: Certificate validity period in days (default: 365)
        key_size: RSA key size in bits (default: 2048)
    """
    
    print(f"Generating SSL certificate for hostname: {hostname}")
    print(f"Key size: {key_size} bits")
    print(f"Valid for: {days_valid} days")
    
    # Generate private key
    print("Generating private key...")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )
    
    # Create certificate subject and issuer
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MQTT Broker"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "IoT Department"),
        x509.NameAttribute(NameOID.COMMON_NAME, hostname),
    ])
    
    # Create Subject Alternative Names (SAN)
    san_list = [
        x509.DNSName(hostname),
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
    ]
    
    # Add additional hostnames if provided
    if hostname != "localhost" and hostname != "127.0.0.1":
        try:
            # Try to parse as IP address
            ip = ipaddress.ip_address(hostname)
            if ip not in [ipaddress.IPv4Address("127.0.0.1")]:
                san_list.append(x509.IPAddress(ip))
        except ValueError:
            # It's a hostname, already added above
            pass
    
    # Create certificate
    print("Creating certificate...")
    cert = x509.CertificateBuilder() \
        .subject_name(subject) \
        .issuer_name(issuer) \
        .public_key(private_key.public_key()) \
        .serial_number(x509.random_serial_number()) \
        .not_valid_before(datetime.datetime.utcnow()) \
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=days_valid)) \
        .add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False,
        ) \
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                content_commitment=False,
                data_encipherment=False,
                encipher_only=False,
                decipher_only=False
            ),
            critical=True,
        ) \
        .add_extension(
            x509.ExtendedKeyUsage([
                x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
            ]),
            critical=True,
        ) \
        .sign(private_key, hashes.SHA256())
    
    # Write certificate to PEM file
    print(f"Writing certificate to: {cert_file}")
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    # Write private key to PEM file
    print(f"Writing private key to: {key_file}")
    with open(key_file, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    # Set appropriate file permissions (read-only for owner)
    os.chmod(cert_file, 0o600)
    os.chmod(key_file, 0o600)
    
    print("\n✅ SSL Certificate generated successfully!")
    print(f"📄 Certificate: {cert_file}")
    print(f"🔑 Private Key: {key_file}")
    print(f"🌐 Hostname: {hostname}")
    print(f"📅 Valid until: {datetime.datetime.utcnow() + datetime.timedelta(days=days_valid)}")
    
    # Display certificate information
    print(f"\n📋 Certificate Details:")
    print(f"   Subject: {cert.subject.rfc4514_string()}")
    print(f"   Serial Number: {cert.serial_number}")
    print(f"   Signature Algorithm: {cert.signature_algorithm_oid._name}")
    print(f"   Public Key Size: {private_key.key_size} bits")
    
    return cert_file, key_file

def display_certificate_info(cert_file):
    """Display information about an existing certificate"""
    try:
        with open(cert_file, "rb") as f:
            cert_data = f.read()
        
        cert = x509.load_pem_x509_certificate(cert_data)
        
        print(f"\n📋 Certificate Information for: {cert_file}")
        print(f"   Subject: {cert.subject.rfc4514_string()}")
        print(f"   Issuer: {cert.issuer.rfc4514_string()}")
        print(f"   Serial Number: {cert.serial_number}")
        print(f"   Valid From: {cert.not_valid_before}")
        print(f"   Valid Until: {cert.not_valid_after}")
        print(f"   Signature Algorithm: {cert.signature_algorithm_oid._name}")
        
        # Display Subject Alternative Names
        try:
            san_ext = cert.extensions.get_extension_for_oid(x509.oid.ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
            print(f"   Subject Alternative Names:")
            for san in san_ext.value:
                print(f"     - {san}")
        except x509.ExtensionNotFound:
            print("   Subject Alternative Names: None")
        
        # Check if certificate is still valid
        now = datetime.datetime.utcnow()
        if now < cert.not_valid_before:
            print("   ⚠️  Certificate is not yet valid")
        elif now > cert.not_valid_after:
            print("   ❌ Certificate has expired")
        else:
            days_left = (cert.not_valid_after - now).days
            print(f"   ✅ Certificate is valid ({days_left} days remaining)")
            
    except FileNotFoundError:
        print(f"❌ Certificate file not found: {cert_file}")
    except Exception as e:
        print(f"❌ Error reading certificate: {e}")

def main():
    parser = argparse.ArgumentParser(description='Generate SSL certificates for MQTT broker')
    parser.add_argument('--hostname', default='localhost', 
                       help='Hostname for the certificate (default: localhost)')
    parser.add_argument('--cert', default='mqtt_server.pem',
                       help='Output certificate file (default: mqtt_server.pem)')
    parser.add_argument('--key', default='mqtt_server_key.pem',
                       help='Output private key file (default: mqtt_server_key.pem)')
    parser.add_argument('--days', type=int, default=365,
                       help='Certificate validity period in days (default: 365)')
    parser.add_argument('--key-size', type=int, default=2048, choices=[1024, 2048, 4096],
                       help='RSA key size in bits (default: 2048)')
    parser.add_argument('--info', 
                       help='Display information about an existing certificate file')
    parser.add_argument('--force', action='store_true',
                       help='Overwrite existing certificate files')
    
    args = parser.parse_args()
    
    if args.info:
        display_certificate_info(args.info)
        return
    
    # Check if files already exist
    if not args.force and (os.path.exists(args.cert) or os.path.exists(args.key)):
        print(f"❌ Certificate files already exist:")
        if os.path.exists(args.cert):
            print(f"   {args.cert}")
        if os.path.exists(args.key):
            print(f"   {args.key}")
        print("Use --force to overwrite or specify different filenames.")
        return
    
    try:
        generate_certificate(
            hostname=args.hostname,
            cert_file=args.cert,
            key_file=args.key,
            days_valid=args.days,
            key_size=args.key_size
        )
        
        print(f"\n🚀 Usage Instructions:")
        print(f"   Start MQTT broker with SSL:")
        print(f"   python mqtt_broker.py --cert {args.cert} --key {args.key}")
        print(f"\n   Connect with mosquitto client:")
        print(f"   mosquitto_pub -h {args.hostname} -p 8883 --cafile {args.cert} -t test -m 'Hello SSL'")
        print(f"   mosquitto_sub -h {args.hostname} -p 8883 --cafile {args.cert} -t test")
        
    except Exception as e:
        print(f"❌ Error generating certificate: {e}")

if __name__ == "__main__":
    main()


import asyncio
import socket
import json
import uuid

BROADCAST_PORT = 5000
BROADCAST_IP = '255.255.255.255'

# Generate a unique ID so we don't accidentally discover ourselves
NODE_ID = str(uuid.uuid4()) 
KNOWN_PEERS = {}

class PeerDiscoveryProtocol(asyncio.DatagramProtocol):
    """Handles incoming UDP broadcast packets."""
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            # Decode the incoming packet
            message = json.loads(data.decode())
            peer_id = message.get("node_id")
            tcp_port = message.get("tcp_port")
            ip = addr[0]

            # If the packet is from a new peer (not ourselves), save it
            if peer_id != NODE_ID and peer_id not in KNOWN_PEERS:
                print(f"\n[+] New Peer Discovered: {ip}:{tcp_port} (ID: {peer_id[:8]})")
                KNOWN_PEERS[peer_id] = {"ip": ip, "tcp_port": tcp_port}
                
        except json.JSONDecodeError:
            pass # Ignore corrupted packets

async def broadcast_presence(transport, tcp_port):
    """Shouts our existence to the local network every 5 seconds."""
    message = json.dumps({"node_id": NODE_ID, "tcp_port": tcp_port}).encode()
    
    while True:
        transport.sendto(message, (BROADCAST_IP, BROADCAST_PORT))
        await asyncio.sleep(5)

async def main():
    loop = asyncio.get_running_loop()
    
    # Configure the UDP socket for broadcasting and port reuse
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', BROADCAST_PORT))

    print(f"[*] Node ID initialized: {NODE_ID[:8]}")
    print(f"[*] Radar active: Listening for peer broadcasts on UDP {BROADCAST_PORT}...")

    # Start the UDP listening server
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PeerDiscoveryProtocol(),
        sock=sock
    )

    # In Phase 2, this node will listen for real messages on this TCP port
    my_tcp_port = 8000 
    
    # Start the shouting loop in the background
    asyncio.create_task(broadcast_presence(transport, my_tcp_port))

    # Keep the script running indefinitely
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        transport.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down mesh node.")
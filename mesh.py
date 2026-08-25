import asyncio
import socket
import json
import uuid

BROADCAST_PORT = 5000
BROADCAST_IP = '255.255.255.255'

NODE_ID = str(uuid.uuid4())
KNOWN_PEERS = {}
ACTIVE_CONNECTIONS = {} # Stores TCP writer objects for active tunnels

# ---------------------------------------------------------
# TCP SERVER & CLIENT LOGIC (PHASE 2)
# ---------------------------------------------------------

async def handle_tcp_connection(reader, writer):
    """Fired whenever a new TCP connection is established (incoming or outgoing)."""
    addr = writer.get_extra_info('peername')
    
    try:
        while True:
            # Wait for data over the TCP tunnel
            data = await reader.read(1024)
            if not data:
                break # Connection closed by peer
            
            print(f"\n[TCP MESSAGE from {addr[0]}]: {data.decode()}")
            
    except ConnectionResetError:
        pass # Peer disconnected
    finally:
        print(f"\n[-] TCP Connection lost: {addr}")
        writer.close()
        await writer.wait_closed()
        # Note: In a production app, we would remove them from ACTIVE_CONNECTIONS here

async def connect_to_peer(ip, port, peer_id):
    """Acts as a client to open a TCP tunnel to a newly discovered peer."""
    if peer_id in ACTIVE_CONNECTIONS:
        return # We already have a tunnel to this peer

    try:
        # Open the TCP connection
        reader, writer = await asyncio.open_connection(ip, port)
        ACTIVE_CONNECTIONS[peer_id] = writer
        print(f"\n[+] TCP Tunnel Established with {peer_id[:8]} at {ip}:{port}")
        
        # Send a test message through the new tunnel
        test_msg = f"Hello from Node {NODE_ID[:8]}!"
        writer.write(test_msg.encode())
        await writer.drain() # Ensure the data is pushed out of the buffer
        
        # Start listening for messages coming back through this tunnel
        asyncio.create_task(handle_tcp_connection(reader, writer))
        
    except Exception as e:
        print(f"\n[-] Failed to establish TCP with {peer_id[:8]}: {e}")

# ---------------------------------------------------------
# UDP DISCOVERY LOGIC (PHASE 1 UPDATED)
# ---------------------------------------------------------

class PeerDiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop):
        self.loop = loop # We need the event loop to schedule TCP connections

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode())
            peer_id = message.get("node_id")
            tcp_port = message.get("tcp_port")
            ip = addr[0]

            if peer_id != NODE_ID and peer_id not in KNOWN_PEERS:
                print(f"\n[*] UDP Radar found peer: {ip}:{tcp_port} (ID: {peer_id[:8]})")
                KNOWN_PEERS[peer_id] = {"ip": ip, "tcp_port": tcp_port}
                
                # Phase 2 Trigger: Instantly attempt a TCP connection
                self.loop.create_task(connect_to_peer(ip, tcp_port, peer_id))
                
        except json.JSONDecodeError:
            pass

async def broadcast_presence(transport, tcp_port):
    message = json.dumps({"node_id": NODE_ID, "tcp_port": tcp_port}).encode()
    while True:
        transport.sendto(message, (BROADCAST_IP, BROADCAST_PORT))
        await asyncio.sleep(5)

# ---------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------

async def main():
    loop = asyncio.get_running_loop()
    
    # 1. Start the TCP Server (To accept incoming tunnels)
    # We use port 0 to let the OS assign a random available port automatically
    tcp_server = await asyncio.start_server(handle_tcp_connection, '0.0.0.0', 0)
    my_tcp_port = tcp_server.sockets[0].getsockname()[1]
    
    print(f"[*] Node ID: {NODE_ID[:8]}")
    print(f"[*] TCP Server listening on port {my_tcp_port}")

    # 2. Start the UDP Radar
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', BROADCAST_PORT))

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PeerDiscoveryProtocol(loop),
        sock=sock
    )

    asyncio.create_task(broadcast_presence(transport, my_tcp_port))

    # Keep alive
    try:
        async with tcp_server:
            await tcp_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        transport.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down mesh node.")
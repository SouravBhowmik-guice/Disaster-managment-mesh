import asyncio
import socket
import json
import uuid

BROADCAST_PORT = 5000
BROADCAST_IP = '255.255.255.255'

NODE_ID = str(uuid.uuid4())
KNOWN_PEERS = {}
ACTIVE_CONNECTIONS = {} 

# PHASE 3: Loop Prevention Cache
SEEN_MESSAGES = set()

# ---------------------------------------------------------
# ROUTING & MESH LOGIC (PHASE 3)
# ---------------------------------------------------------

async def flood_message(msg_dict, exclude_writer=None):
    """Sends a message to all active TCP connections."""
    # We append a newline '\n' to frame the JSON packets over the TCP stream
    payload = (json.dumps(msg_dict) + "\n").encode()
    
    # Iterate through a copy of the dictionary items
    for peer_id, writer in list(ACTIVE_CONNECTIONS.items()):
        if writer != exclude_writer:
            try:
                writer.write(payload)
                await writer.drain()
            except Exception as e:
                print(f"[-] Failed to route to {peer_id[:8]}: {e}")

async def originate_message(target_id, content):
    """Creates a brand new message and injects it into the mesh."""
    msg_id = str(uuid.uuid4())
    SEEN_MESSAGES.add(msg_id) # Prevent echoing our own message back to ourselves
    
    msg_dict = {
        "msg_id": msg_id,
        "sender_id": NODE_ID,
        "target_id": target_id,
        "content": content
    }
    
    await flood_message(msg_dict)

# ---------------------------------------------------------
# TCP SERVER & CLIENT LOGIC
# ---------------------------------------------------------

async def handle_tcp_connection(reader, writer):
    addr = writer.get_extra_info('peername')
    
    try:
        while True:
            # Use readline() to ensure we get a complete JSON string 
            data = await reader.readline()
            if not data:
                break 
            
            try:
                msg_dict = json.loads(data.decode().strip())
            except json.JSONDecodeError:
                continue # Ignore corrupted data
                
            msg_id = msg_dict.get("msg_id")
            
            # 1. DEDUPLICATION (Loop Prevention)
            if msg_id in SEEN_MESSAGES:
                continue # We already routed this packet, drop it!
                
            SEEN_MESSAGES.add(msg_id)
            
            target_id = msg_dict.get("target_id")
            sender_id = msg_dict.get("sender_id")
            content = msg_dict.get("content")

            # 2. IS IT FOR ME?
            if target_id == NODE_ID or target_id == "ALL":
                print(f"\n[INBOX - {sender_id[:8]}]: {content}")
            
            # 3. ROUTING (Relay it to others)
            if target_id != NODE_ID:
                print(f"\n[*] Relaying packet {msg_id[:4]}... to the mesh")
                # Forward to everyone EXCEPT the node that just sent it to us
                await flood_message(msg_dict, exclude_writer=writer)
            
    except ConnectionResetError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()

async def connect_to_peer(ip, port, peer_id):
    if peer_id in ACTIVE_CONNECTIONS:
        return 

    try:
        reader, writer = await asyncio.open_connection(ip, port)
        ACTIVE_CONNECTIONS[peer_id] = writer
        print(f"\n[+] Connected to {peer_id[:8]}")
        
        # Start listening for messages coming back through this tunnel
        asyncio.create_task(handle_tcp_connection(reader, writer))
        
        # Inject a broadcast test message to the mesh
        await originate_message("ALL", "Hello Mesh Network!")
        
    except Exception as e:
        pass

# ---------------------------------------------------------
# UDP DISCOVERY LOGIC 
# ---------------------------------------------------------

class PeerDiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop):
        self.loop = loop

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode())
            peer_id = message.get("node_id")
            tcp_port = message.get("tcp_port")
            ip = addr[0]

            if peer_id != NODE_ID and peer_id not in KNOWN_PEERS:
                KNOWN_PEERS[peer_id] = {"ip": ip, "tcp_port": tcp_port}
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
    
    tcp_server = await asyncio.start_server(handle_tcp_connection, '0.0.0.0', 0)
    my_tcp_port = tcp_server.sockets[0].getsockname()[1]
    
    print(f"[*] Node ID: {NODE_ID[:8]}")
    print(f"[*] TCP Server listening on port {my_tcp_port}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', BROADCAST_PORT))

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PeerDiscoveryProtocol(loop),
        sock=sock
    )

    asyncio.create_task(broadcast_presence(transport, my_tcp_port))

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
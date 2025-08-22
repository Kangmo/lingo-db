#!/usr/bin/env python3
import json
import socket
import sys

def send_mcp_command(command):
    # MCP uses JSON-RPC 2.0
    request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "lldb_command",
            "arguments": {
                "debugger_id": 1,
                "command": command
            }
        },
        "id": 1
    }
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 59999))
        message = json.dumps(request) + "\n"
        s.sendall(message.encode())
        response = s.recv(4096)
        return response.decode()

# Test connection
try:
    print("Creating target...")
    result = send_mcp_command("target create build/lingodb-debug/sql")
    print(result)
except Exception as e:
    print(f"Error: {e}")
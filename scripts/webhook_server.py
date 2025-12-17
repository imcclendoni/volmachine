#!/usr/bin/env python3
"""
VolMachine Webhook Server - Remote Trade Execution

Allows Streamlit Cloud to execute trades on your local machine via IBKR Gateway.

Usage:
    1. Start IBKR Gateway on port 4002
    2. python3 scripts/webhook_server.py
    3. ngrok http 8765
    4. Set WEBHOOK_URL in Streamlit secrets

Security:
    - Requires WEBHOOK_TOKEN for authentication
    - Paper mode only
    - Valid symbols only (SPY, QQQ, IWM, TLT)
"""

import os
import subprocess
import secrets
from pathlib import Path
from datetime import datetime

try:
    from fastapi import FastAPI, HTTPException, Header
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    print("Installing required packages...")
    subprocess.run(['pip', 'install', 'fastapi', 'uvicorn', 'pydantic'], check=True)
    from fastapi import FastAPI, HTTPException, Header
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn

# Configuration
PORT = int(os.getenv('WEBHOOK_PORT', 8765))
TOKEN = os.getenv('WEBHOOK_TOKEN', None)
VALID_SYMBOLS = {'SPY', 'QQQ', 'IWM', 'TLT'}
PROJECT_ROOT = Path(__file__).parent.parent

# Generate token if not set
if not TOKEN:
    TOKEN = secrets.token_urlsafe(32)
    print(f"\n⚠️  No WEBHOOK_TOKEN set. Generated temporary token:")
    print(f"    {TOKEN}")
    print(f"\n    Add to .streamlit/secrets.toml:")
    print(f'    WEBHOOK_TOKEN = "{TOKEN}"')
    print()

app = FastAPI(title="VolMachine Webhook Server", version="1.0")

# CORS for Streamlit Cloud
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ExecuteRequest(BaseModel):
    symbol: str
    action: str = "submit"  # "preview" or "submit"


class ExecuteResponse(BaseModel):
    success: bool
    symbol: str
    action: str
    output: str
    timestamp: str


def verify_token(authorization: str = Header(None)):
    """Verify the bearer token."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing authorization header")
    
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    provided_token = authorization.replace("Bearer ", "")
    if provided_token != TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    
    return True


@app.get("/")
async def root():
    """Health check."""
    return {
        "status": "online",
        "service": "VolMachine Webhook Server",
        "valid_symbols": list(VALID_SYMBOLS),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/status")
async def status():
    """Check server status and IBKR connectivity."""
    return {
        "status": "online",
        "ibkr_port": 4002,
        "symbols": list(VALID_SYMBOLS),
        "timestamp": datetime.now().isoformat()
    }


@app.post("/execute", response_model=ExecuteResponse)
async def execute_trade(request: ExecuteRequest, authorization: str = Header(None)):
    """Execute a trade via IBKR."""
    verify_token(authorization)
    
    # Validate symbol
    symbol = request.symbol.upper()
    if symbol not in VALID_SYMBOLS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid symbol: {symbol}. Valid: {list(VALID_SYMBOLS)}"
        )
    
    # Determine action flag
    if request.action == "preview":
        flag = "--dry-run"
    elif request.action == "submit":
        flag = "--submit"
    else:
        raise HTTPException(status_code=400, detail="Invalid action. Use 'preview' or 'submit'")
    
    # Build command
    cmd = ['python3', 'scripts/submit_test_order.py', '--paper', flag, '--symbol', symbol]
    
    print(f"\n🚀 Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(PROJECT_ROOT)
        )
        
        output = result.stdout + result.stderr
        success = result.returncode == 0
        
        if success:
            print(f"✅ {symbol} {request.action} successful")
        else:
            print(f"❌ {symbol} {request.action} failed")
        
        return ExecuteResponse(
            success=success,
            symbol=symbol,
            action=request.action,
            output=output[-5000:],  # Last 5000 chars
            timestamp=datetime.now().isoformat()
        )
        
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Execution timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════════╗
║          VOLMACHINE WEBHOOK SERVER                       ║
╠══════════════════════════════════════════════════════════╣
║  Port: {PORT:<50} ║
║  Token: {TOKEN[:20]}...{TOKEN[-8:]:<25} ║
╠══════════════════════════════════════════════════════════╣
║  Endpoints:                                              ║
║    GET  /         - Health check                         ║
║    GET  /status   - Server status                        ║
║    POST /execute  - Execute trade                        ║
╠══════════════════════════════════════════════════════════╣
║  Next steps:                                             ║
║    1. Start IBKR Gateway on port 4002                    ║
║    2. Run: ngrok http {PORT:<41} ║
║    3. Add ngrok URL to .streamlit/secrets.toml           ║
╚══════════════════════════════════════════════════════════╝
""")
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)

import asyncio
import websockets
from websockets.client import ClientProtocol
import json
import httpx
import logging
from typing import AsyncGenerator
from worker.config import WorkerConfig
from common.models import MessageType, ProxyRequest, ResponseStart, ResponseChunk, ResponseEnd, ErrorMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RequestHandler:
    """Handles HTTP requests to target API"""
    
    def __init__(self, config: WorkerConfig):
        self.config = config
    
    async def process(self, request_data: dict) -> AsyncGenerator[dict, None]:
        """Process request and yield response parts"""
        
        # Parse and validate request
        try:
            proxy_req = ProxyRequest(**request_data)
        except Exception as e:
            logger.error(f"Invalid request data: {e}")
            yield ErrorMessage(
                request_id=request_data.get("request_id", "unknown"),
                error=f"Invalid request format: {str(e)}"
            ).model_dump()
            return
        
        # Prepare request
        headers = proxy_req.headers.copy()
        headers.pop("host", None)
        
        url = f"{self.config.target_hostname}{proxy_req.path}"
        logger.info(f"Processing request {proxy_req.request_id}: {proxy_req.method} {url}")
        
        try:
            async for response_part in self._make_request(proxy_req, url, headers):
                yield response_part
                
        except Exception as e:
            logger.error(f"Unexpected error for {proxy_req.request_id}: {e}")
            yield ErrorMessage(
                request_id=proxy_req.request_id,
                error=f"Internal error: {str(e)}",
                details={"exception_type": type(e).__name__}
            ).model_dump()
    
    async def _make_request(
        self, 
        proxy_req: ProxyRequest, 
        url: str, 
        headers: dict
    ) -> AsyncGenerator[dict, None]:
        """Make HTTP request and stream response"""
        
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    method=proxy_req.method,
                    url=url,
                    headers=headers,
                    content=proxy_req.body.encode() if proxy_req.body else None,
                    timeout=self.config.request_timeout
                ) as response:
                    
                    # Send response start
                    yield ResponseStart(
                        request_id=proxy_req.request_id,
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        content_type=response.headers.get("content-type", "application/json")
                    ).model_dump()
                    
                    # Stream chunks
                    async for chunk in response.aiter_text():
                        yield ResponseChunk(
                            request_id=proxy_req.request_id,
                            chunk=chunk
                        ).model_dump()
                    
                    # Send response end
                    yield ResponseEnd(
                        request_id=proxy_req.request_id
                    ).model_dump()
                    
        except httpx.TimeoutException as e:
            logger.error(f"Timeout for {proxy_req.request_id}: {e}")
            yield ErrorMessage(
                request_id=proxy_req.request_id,
                error="Request timeout",
                details={"exception": str(e)}
            ).model_dump()
            
        except httpx.RequestError as e:
            logger.error(f"Request error for {proxy_req.request_id}: {e}")
            yield ErrorMessage(
                request_id=proxy_req.request_id,
                error=f"Request failed: {str(e)}",
                details={"exception_type": type(e).__name__}
            ).model_dump()


class ProxyWorker:
    """Worker that connects to proxy server and processes requests"""
    
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.handler = RequestHandler(config)
        self.running = False
    
    async def start(self):
        """Start worker and maintain connection"""
        self.running = True
        logger.info("Starting worker...")
        logger.info(f"Configuration: {self.config.model_dump()}")
        
        while self.running:
            try:
                await self._connect_and_process()
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"WebSocket error: {e}")
                logger.info(f"Reconnecting in {self.config.reconnect_delay} seconds...")
                await asyncio.sleep(self.config.reconnect_delay)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                logger.info(f"Reconnecting in {self.config.reconnect_delay} seconds...")
                await asyncio.sleep(self.config.reconnect_delay)
    
    async def _connect_and_process(self):
        """Connect to proxy server and process messages"""
        async with websockets.connect(self.config.proxy_server_url) as websocket:
            logger.info(f"Connected to proxy server at {self.config.proxy_server_url}")
            logger.info(f"Forwarding requests to {self.config.target_hostname}")
            
            async for message in websocket:
                await self._handle_message(websocket, message)
    
    async def _handle_message(self, websocket: ClientProtocol, message: str):
        """Handle incoming message from proxy server"""
        try:
            request_data = json.loads(message)
            
            if request_data.get("type") == MessageType.REQUEST:
                async for response_part in self.handler.process(request_data):
                    await websocket.send(json.dumps(response_part))
                
                logger.info(f"Response completed for request {request_data.get('request_id')}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            # Try to send error back if we have request_id
            request_data = {}
            try:
                request_data = json.loads(message)
            except:
                pass
                
            if "request_id" in request_data:
                error_msg = ErrorMessage(
                    request_id=request_data["request_id"],
                    error=f"Message handling error: {str(e)}"
                )
                try:
                    await websocket.send(error_msg.model_dump_json())
                except:
                    logger.error("Failed to send error message back")
    
    def stop(self):
        """Stop worker"""
        self.running = False
        logger.info("Worker stopping...")


# Main entry point
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Worker")
    parser.add_argument("--server-url", type=str)
    parser.add_argument("--target-hostname", type=str)
    args = parser.parse_args()

    config = WorkerConfig()

    if args.server_url:
        config.proxy_server_url = args.server_url
    if args.target_hostname:
        config.target_hostname = args.target_hostname

    logging.info(f"Proxy Server: {config.proxy_server_url}")
    logging.info(f"Target Hostname: {config.target_hostname}")
    
    worker = ProxyWorker(config)
    
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        worker.stop()
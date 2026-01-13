from pydantic import BaseModel, Field

class WorkerConfig(BaseModel):
    """Worker configuration"""
    proxy_server_url: str = Field(default="ws://localhost:8080/worker")
    target_hostname: str = Field(default="http://localhost:11434")
    reconnect_delay: int = Field(default=5, description="Delay in seconds before reconnecting")
    request_timeout: float = Field(default=30.0, description="Request timeout in seconds")
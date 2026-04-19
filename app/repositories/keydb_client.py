import redis.asyncio as redis
from typing import Optional, Dict
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

class KeyDbClient:
    """Асинхронный клиент для работы с KeyDB (общий для всех вариантов)"""
    def __init__(self, url: str):
        self.url = url
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        try:
            self.client = redis.from_url(
                self.url,
                decode_responses=False,
                max_connections=settings.KEYDB_MAX_CONNECTIONS,
                socket_connect_timeout=settings.KEYDB_CONNECT_TIMEOUT,
                socket_timeout=settings.KEYDB_READ_TIMEOUT
            )
            await self.client.ping()
            logger.info(f"Connected to KeyDB at {self.url}")
        except Exception as e:
            logger.error(f"Failed to connect to KeyDB: {e}")
            raise

    async def disconnect(self) -> None:
        if self.client:
            await self.client.close()
            logger.info("Disconnected from KeyDB")

    async def get(self, key: str) -> Optional[bytes]:
        try:
            return await self.client.get(key)
        except Exception as e:
            logger.error(f"GET failed for key {key}: {e}")
            raise

    async def hget(self, key: str, field: str) -> Optional[bytes]:
        try:
            return await self.client.hget(key, field)
        except Exception as e:
            logger.error(f"HGET failed for key {key}, field {field}: {e}")
            raise

    async def hgetall(self, key: str) -> Dict[str, bytes]:
        try:
            return await self.client.hgetall(key)
        except Exception as e:
            logger.error(f"HGETALL failed for key {key}: {e}")
            raise

    def pipeline(self, transaction: bool = True):
        """Транзакционный пайплайн (как в common_rank_wrapper)."""
        if self.client is None:
            raise RuntimeError("KeyDbClient is not connected")
        return self.client.pipeline(transaction=transaction)
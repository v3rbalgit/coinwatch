import json
from typing import Any, Callable, Dict, Optional, cast, Awaitable
from aio_pika import (
    Message,
    ExchangeType,
    connect_robust,
    RobustConnection,
    RobustChannel,
    RobustExchange
)
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from shared.utils.logger import LoggerSetup
from .schemas import BaseMessage

logger = LoggerSetup.setup(__name__)

MessageHandler = Callable[[Dict[str, Any]], Awaitable[None]]

class MessageBroker:
    """
    RabbitMQ message broker for inter-service communication.

    Provides:
    - Topic-based pub/sub messaging
    - Message serialization/deserialization
    - Automatic reconnection
    - Dead letter handling
    """
    def __init__(self, service_name: str):
        self.service_name = service_name
        self._connection: Optional[RobustConnection] = None
        self._channel: Optional[RobustChannel] = None
        self._exchange: Optional[RobustExchange] = None
        self._dlx: Optional[RobustExchange] = None
        self._handlers: Dict[str, MessageHandler] = {}
        self._queues: Dict[str, AbstractQueue] = {}

    async def connect(self, url: str = "amqp://guest:guest@rabbitmq/") -> None:
        """Connect to RabbitMQ server"""
        try:
            # Create robust connection that automatically reconnects
            connection = await connect_robust(url)
            self._connection = cast(RobustConnection, connection)

            channel = await self._connection.channel()
            self._channel = cast(RobustChannel, channel)

            # Declare topic exchange
            exchange = await self._channel.declare_exchange(
                "coinwatch",
                ExchangeType.TOPIC,
                durable=True
            )
            self._exchange = cast(RobustExchange, exchange)

            # Declare dead letter exchange
            dlx = await self._channel.declare_exchange(
                "coinwatch.dlx",
                ExchangeType.TOPIC,
                durable=True
            )
            self._dlx = cast(RobustExchange, dlx)

            logger.info(f"Connected to RabbitMQ: {self.service_name}")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def close(self) -> None:
        """Close connection"""
        if self._connection:
            await self._connection.close()
            self._connection = None
            self._channel = None
            self._exchange = None
            self._dlx = None
            self._queues.clear()

    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Publish message to a topic.

        Args:
            topic: Routing key (e.g., "market.symbol.added")
            message: Message payload
        """
        if not self._exchange:
            raise RuntimeError("Not connected to RabbitMQ")

        try:
            # Create message with metadata
            message_body = {
                "service": self.service_name,
                "topic": topic,
                "payload": message
            }

            # Publish with persistent delivery and message TTL
            await self._exchange.publish(
                Message(
                    body=json.dumps(message_body).encode(),
                    delivery_mode=2,  # Persistent
                    expiration=60000,  # 1 minute TTL
                    headers={
                        "x-dead-letter-exchange": "coinwatch.dlx",
                        "x-dead-letter-routing-key": f"dlx.{topic}"
                    }
                ),
                routing_key=topic
            )

            logger.debug(f"Published message to {topic}: {message}")

        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise

    async def subscribe(self, topic: str, callback: MessageHandler) -> None:
        """
        Subscribe to a topic with a callback.

        Args:
            topic: Topic pattern (e.g., "market.symbol.*")
            callback: Async function to handle messages
        """
        if not self._channel or not self._exchange:
            raise RuntimeError("Not connected to RabbitMQ")

        try:
            # Declare queue with dead letter config
            queue = await self._channel.declare_queue(
                f"{self.service_name}.{topic}",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "coinwatch.dlx",
                    "x-dead-letter-routing-key": f"dlx.{topic}"
                }
            )

            # Bind queue to exchange
            await queue.bind(self._exchange, topic)

            # Store handler and queue
            self._handlers[topic] = callback
            self._queues[topic] = queue

            # Start consuming
            await queue.consume(self._message_handler)

            logger.info(f"Subscribed to {topic}")

        except Exception as e:
            logger.error(f"Failed to subscribe to {topic}: {e}")
            raise

    async def _message_handler(self, message: AbstractIncomingMessage) -> None:
        """Handle incoming messages"""
        async with message.process():
            try:
                # Parse message
                body = json.loads(message.body.decode())
                topic = body["topic"]
                payload = body["payload"]

                # Find handler
                handler = self._handlers.get(topic)
                if handler:
                    await handler(payload)
                else:
                    logger.warning(f"No handler for topic: {topic}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Message will be dead-lettered
                raise

    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic"""
        if not self._channel:
            return

        try:
            # Get queue
            queue = self._queues.get(topic)
            if queue:
                # Remove all consumers
                await queue.purge()
                await queue.delete()
                self._queues.pop(topic)

            # Remove handler
            self._handlers.pop(topic, None)

            logger.info(f"Unsubscribed from {topic}")

        except Exception as e:
            logger.error(f"Failed to unsubscribe from {topic}: {e}")
            raise

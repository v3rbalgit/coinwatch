# src/adapters/telegram.py

from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel, InputChannel
from datetime import datetime, timezone, timedelta

from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class TelegramAdapter:
    """
    Adapter for Telegram data using Telethon.
    Handles channel access and data transformation.
    """

    def __init__(self, api_id: int, api_hash: str, session_name: str = "coinwatch_bot"):
        """
        Initialize the Telegram adapter.

        Args:
            api_id (int): Telegram API ID
            api_hash (str): Telegram API hash
            session_name (str): Name for the session file
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name

    def _extract_channel_username(self, url: str) -> str:
        """Extract channel username from URL"""
        # Handle t.me URLs
        if 't.me/' in url:
            return url.split('t.me/')[-1].split('/')[0]

        # Handle telegram.me URLs
        if 'telegram.me/' in url:
            return url.split('telegram.me/')[-1].split('/')[0]

        # Handle direct usernames
        return url.lstrip('@')

    def _get_input_channel(self, channel: Channel) -> InputChannel:
        """Convert Channel to InputChannel for API calls"""
        return InputChannel(
            channel_id=channel.id,
            access_hash=channel.access_hash or 0
        )

    async def get_channel_metrics(self, channel_url: str) -> dict | None:
        """
        Get metrics for a Telegram channel.

        Args:
            channel_url (str): URL or username of the channel

        Returns:
            Optional[Dict]: Channel metrics including member count, online members, etc.
        """
        try:
            async with TelegramClient(self.session_name, self.api_id, self.api_hash) as client:
                username = self._extract_channel_username(channel_url)
                entity = await client.get_entity(username)

                if not isinstance(entity, Channel):
                    logger.error(f"Entity {username} is not a channel")
                    return None

                # Convert to input channel and get full info
                input_channel = self._get_input_channel(entity)
                full = await client(GetFullChannelRequest(channel=input_channel))

                return {
                    'members': getattr(entity, 'participants_count', 0),
                    'online_members': getattr(full.full_chat, 'online_count', 0)
                }

        except Exception as e:
            logger.error(f"Error fetching Telegram channel metrics: {e}")
            return None

    async def get_recent_messages(self, channel_url: str, hours: int = 24) -> dict | None:
        """
        Get recent messages from a channel.

        Args:
            channel_url (str): URL or username of the channel
            hours (int): Hours of messages to fetch

        Returns:
            Optional[Dict]: Message metrics and content for sentiment analysis
        """
        try:
            async with TelegramClient(self.session_name, self.api_id, self.api_hash) as client:
                username = self._extract_channel_username(channel_url)
                entity = await client.get_entity(username)

                if not isinstance(entity, Channel):
                    logger.error(f"Entity {username} is not a channel")
                    return None

                # Calculate cutoff time
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

                # Get recent messages
                messages = []
                message_texts = []

                async for message in client.iter_messages(entity, limit=100):
                    if message.date < cutoff_time:
                        break

                    if message.message:  # Only include text messages
                        messages.append(message)
                        message_texts.append(message.message)

                return {
                    'messages_24h': len(messages),
                    'texts': message_texts
                }

        except Exception as e:
            logger.error(f"Error fetching Telegram messages: {e}")
            return None

from setuptools import setup, find_packages

setup(
    name="coinwatch-shared",
    version="1.0.0",
    description="Shared package for Coinwatch services",
    author="Boris Vereš",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=1.4.0",
        "asyncpg>=0.25.0",
        "pydantic>=1.8.0",
        "python-dotenv>=0.19.0",
        "aio-pika>=8.0.0",  # For RabbitMQ
        "aiohttp>=3.8.0",   # For HTTP client
        "websockets>=14.1",  # For WebSocket client
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-asyncio>=0.15.0",
            "black>=21.0.0",
            "isort>=5.0.0",
        ]
    }
)

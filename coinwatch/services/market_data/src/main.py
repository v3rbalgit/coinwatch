import os
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .service import MarketDataService
from shared.core.config import Config
from shared.core.enums import ServiceStatus, Interval
from shared.database.connection import DatabaseConnection
from shared.database.repositories import SymbolRepository, KlineRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, ServiceStatusMessage
from shared.clients.exchanges import BybitAdapter
from shared.clients.registry import ExchangeAdapterRegistry
from shared.utils.logger import LoggerSetup
import shared.utils.time as TimeUtils
from shared.managers.kline import KlineManager
from shared.managers.indicator import IndicatorManager

logger = LoggerSetup.setup(__name__)

service: Optional[MarketDataService] = None
metrics_task: Optional[asyncio.Task] = None
kline_manager: Optional[KlineManager] = None
indicator_manager: Optional[IndicatorManager] = None

async def publish_metrics():
    """Periodically publish service metrics"""
    while True:
        try:
            if service and service._status == ServiceStatus.RUNNING:
                # Collect current metrics
                recent_errors = service._error_tracker.get_recent_errors(60)
                collection_errors = len([e for e in recent_errors if "collection" in str(e).lower()])
                streaming_errors = len([e for e in recent_errors if "streaming" in str(e).lower()])

                uptime = 0.0
                if service._start_time is not None:
                    uptime = (TimeUtils.get_current_timestamp() - service._start_time) / 1000

                # Publish service status with metrics
                await service.message_broker.publish(
                    MessageType.SERVICE_STATUS,
                    ServiceStatusMessage(
                        service="market_data",
                        type=MessageType.SERVICE_STATUS,
                        timestamp=TimeUtils.get_current_timestamp(),
                        status=service._status.value,
                        uptime=uptime,
                        error_count=len(recent_errors),
                        warning_count=len([e for e in recent_errors if "warning" in str(e).lower()]),
                        metrics={
                            "active_symbols": len(service._active_symbols),
                            "active_collections": len(service.data_collector._processing_symbols),
                            "streaming_symbols": len(service.data_collector._streaming_symbols),
                            "streaming_errors": streaming_errors,
                            "collection_errors": collection_errors,
                            "batch_size": service._batch_size,
                            "last_error": str(service._last_error) if service._last_error else None
                        }
                    ).model_dump()
                )

        except Exception as e:
            logger.error(f"Error publishing metrics: {e}")

        # Sleep for metrics interval
        await asyncio.sleep(30)  # Configurable interval

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Service lifecycle manager"""
    global service, metrics_task, kline_manager, indicator_manager

    try:
        config = Config()

        # Initialize database connection
        db = DatabaseConnection(config.database, schema="market_data")
        await db.initialize()

        # Initialize repositories
        symbol_repository = SymbolRepository(db)
        kline_repository = KlineRepository(db)

        # Initialize exchange registry with adapters
        exchange_registry = ExchangeAdapterRegistry()
        exchange_registry.register("bybit", BybitAdapter(config.adapters.bybit))

        # Initialize message broker
        message_broker = MessageBroker("market_data")

        # Initialize managers
        kline_manager = KlineManager(message_broker, kline_repository, config.redis_url)
        indicator_manager = IndicatorManager(config.redis_url)

        service = MarketDataService(
            symbol_repository=symbol_repository,
            kline_repository=kline_repository,
            exchange_registry=exchange_registry,
            message_broker=message_broker,
            config=config.market_data
        )

        # Start service and metrics publishing
        await service.start()
        metrics_task = asyncio.create_task(publish_metrics())

        yield  # Service is running

    except Exception as e:
        logger.error(f"Service initialization failed: {e}")
        raise HTTPException(status_code=500, detail=f"Service initialization failed: {str(e)}")

    finally:
        # Cleanup
        if metrics_task:
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass
        if kline_manager:
            await kline_manager.cleanup()
        if indicator_manager:
            await indicator_manager.cleanup()
        if service:
            await db.close()
            await service.stop()

# Initialize FastAPI app
app = FastAPI(
    title="Market Data Service",
    description="Market data collection and management service",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Business endpoints
@app.get("/symbols")
async def get_symbols():
    """Get all trading symbols"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        symbols = sorted(list(service._active_symbols), key=lambda x: x.name)
        return {
            "symbols": [symbol.model_dump() for symbol in symbols]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")

@app.get("/symbols/{symbol}")
async def get_symbol(symbol: str):
    """Get details for a specific symbol"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        # Find symbol in active symbols
        symbol_info = next(
            (s for s in service._active_symbols if s.name == symbol),
            None
        )
        if not symbol_info:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        return symbol_info.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbol details: {str(e)}")

@app.get("/klines/{symbol}")
async def get_klines(
    symbol: str,
    interval: str = Query(..., description="Candlestick interval (e.g., 5, 15, 60, D)"),
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: Optional[int] = None
):
    """
    Get candlestick data for a symbol

    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Candlestick interval (e.g., 5, 15, 60, D)
        start_time: Optional start time in milliseconds
        end_time: Optional end time in milliseconds
        limit: Optional limit on number of candles
    """
    if not service or not kline_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        # Find symbol in active symbols
        symbol_info = next(
            (s for s in service._active_symbols if s.name == symbol),
            None
        )
        if not symbol_info:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        # Convert interval string to Interval enum
        try:
            interval = Interval(interval)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid interval: {interval}. Valid values are: {[tf.value for tf in Interval]}"
            )

        # Get klines using kline manager
        klines = await kline_manager.get_klines(
            symbol=symbol_info,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        return {
            "symbol": symbol,
            "interval": interval.value,
            "klines": [
                {
                    "timestamp": kline.timestamp,
                    "open": str(kline.open_price),
                    "high": str(kline.high_price),
                    "low": str(kline.low_price),
                    "close": str(kline.close_price),
                    "volume": str(kline.volume),
                    "turnover": str(kline.turnover)
                } for kline in klines
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch klines: {str(e)}")

@app.get("/indicators/{symbol}/{indicator}")
async def get_indicator(
    symbol: str,
    indicator: str,
    interval: str = Query(..., description="Candlestick interval (e.g., 5, 15, 60, D)"),
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    length: Optional[int] = None,
    fast_length: Optional[int] = None,
    slow_length: Optional[int] = None,
    signal_length: Optional[int] = None,
    std_dev: Optional[float] = None
):
    """
    Get technical indicator values for a symbol

    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        indicator: Indicator type (rsi, macd, bb, sma, ema, obv)
        interval: Candlestick interval (e.g., 5, 15, 60, D)
        start_time: Optional start time in milliseconds
        end_time: Optional end time in milliseconds
        length: Period length for RSI, BB, SMA, EMA
        fast_length: Fast period for MACD
        slow_length: Slow period for MACD
        signal_length: Signal period for MACD
        std_dev: Standard deviation for Bollinger Bands
    """
    if not service or not kline_manager or not indicator_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        # Find symbol in active symbols
        symbol_info = next(
            (s for s in service._active_symbols if s.name == symbol),
            None
        )
        if not symbol_info:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        # Convert interval string to Interval enum
        try:
            interval = Interval(interval)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid interval: {interval}. Valid values are: {[tf.value for tf in Interval]}"
            )

        # Get klines first
        klines = await kline_manager.get_klines(
            symbol=symbol_info,
            interval=interval,
            start_time=start_time,
            end_time=end_time
        )

        # Calculate indicator based on type
        indicator = indicator.lower()
        result = None

        match indicator:
            case 'rsi':
                result = await indicator_manager.calculate_rsi(klines, length or 14)
            case 'macd':
                result = await indicator_manager.calculate_macd(
                    klines,
                    fast=fast_length or 12,
                    slow=slow_length or 26,
                    signal=signal_length or 9
                )
            case 'bb':
                result = await indicator_manager.calculate_bollinger_bands(
                    klines,
                    length=length or 20,
                    std_dev=std_dev or 2.0
                )
            case 'sma':
                result = await indicator_manager.calculate_sma(klines, period=length or 20)
            case 'ema':
                result = await indicator_manager.calculate_ema(klines, period=length or 20)
            case 'obv':
                result = await indicator_manager.calculate_obv(klines)
            case _:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported indicator: {indicator}"
                )

        return {
            "symbol": symbol,
            "indicator": indicator,
            "interval": interval.value,
            "values": [value.model_dump() for value in result]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate indicator: {str(e)}")

# Service endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    return {
        "status": "healthy",
        "service_status": service._status.value,
        "active_symbols": len(service._active_symbols)
    }

@app.get("/metrics")
async def get_metrics():
    """Get service metrics (for local debugging)"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        uptime = 0.0
        if service._start_time is not None:
            uptime = (TimeUtils.get_current_timestamp() - service._start_time) / 1000

        recent_errors = service._error_tracker.get_recent_errors(60)
        collection_errors = len([e for e in recent_errors if "collection" in str(e).lower()])
        streaming_errors = len([e for e in recent_errors if "streaming" in str(e).lower()])

        return {
            "status": service._status.value,
            "uptime_seconds": uptime,
            "last_error": str(service._last_error) if service._last_error else None,
            "error_count": len(recent_errors),
            "warning_count": len([e for e in recent_errors if "warning" in str(e).lower()]),
            "active_symbols": len(service._active_symbols),
            "active_collections": len(service.data_collector._processing_symbols),
            "streaming_symbols": len(service.data_collector._streaming_symbols),
            "streaming_errors": streaming_errors,
            "collection_errors": collection_errors,
            "batch_size": service._batch_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect metrics: {str(e)}")

@app.get("/status")
async def get_status():
    """Get detailed service status"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    return {
        "status": service.get_service_status()
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=bool(os.getenv("DEBUG", False))
    )

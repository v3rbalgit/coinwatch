# src/monitoring/metric.py

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional
from src.utils.domain_types import ServiceStatus

from prometheus_client import Counter, Gauge, Info, CollectorRegistry

@dataclass
class HealthMetrics:
    """Detailed health metrics suitable for Prometheus"""
    status: ServiceStatus
    uptime_seconds: float
    active_observers: int
    total_observations: int
    last_observation_time: Optional[datetime]
    error_count: int
    observer_states: Dict[str, str]
    memory_usage_mb: float
    observer_metrics: Dict[str, Dict[str, float]]


class MonitoringMetrics:
  """Prometheus metrics for monitoring service"""
  def __init__(self, registry: Optional[CollectorRegistry] = None):
      self.registry = registry or CollectorRegistry()

      # Service metrics
      self.service_up = Gauge(
          'monitoring_service_up',
          'Current status of the monitoring service',
          ['status'],
          registry=self.registry
      )

      self.service_uptime = Counter(
          'monitoring_service_uptime_seconds',
          'Total number of seconds the service has been running',
          registry=self.registry
      )

      self.active_observers = Gauge(
          'monitoring_service_active_observers',
          'Number of currently active observers',
          registry=self.registry
      )

      self.total_observations = Counter(
          'monitoring_service_total_observations',
          'Total number of observations recorded',
          registry=self.registry
      )

      self.error_count = Counter(
          'monitoring_service_error_count',
          'Total number of errors encountered',
          registry=self.registry
      )

      # Resource metrics
      self.memory_usage_bytes = Gauge(
          'monitoring_service_memory_usage_bytes',
          'Current memory usage in bytes',
          registry=self.registry
      )

      # Observer-specific metrics
      self.observer_success_rate = Gauge(
          'monitoring_observer_success_rate',
          'Success rate of observations',
          ['observer'],
          registry=self.registry
      )

      self.observer_last_success = Gauge(
          'monitoring_observer_last_success_timestamp',
          'Timestamp of last successful observation',
          ['observer'],
          registry=self.registry
      )

      # Service info
      self.service_info = Info(
          'monitoring_service',
          'Monitoring service information',
          registry=self.registry
      )


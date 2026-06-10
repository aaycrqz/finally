# Market Data Backend — Implementation Design

Complete implementation guide for the FinAlly market data subsystem. All code lives in `backend/app/market/`. This document is the authoritative reference for building the market data layer; the archive folder contains earlier drafts.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Data — `seed_prices.py`](#6-seed-data)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [SSE Streaming — `stream.py`](#10-sse-streaming)
11. [Package Init — `__init__.py`](#11-package-init)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Error Handling & Edge Cases](#14-error-handling--edge-cases)
15. [Testing](#15-testing)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Architecture Overview

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM price simulation (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼ writes prices via cache.update()
   PriceCache (thread-safe, in-memory, version-counted)
        │
        ├──→ GET /api/stream/prices  (SSE endpoint)
        ├──→ GET /api/portfolio      (portfolio valuation)
        └──→ POST /api/portfolio/trade  (trade execution at current price)
```

**Key design principles:**

- **Strategy pattern**: Both data sources implement the same ABC. All downstream code is source-agnostic — it reads only from the cache.
- **Push model**: Data sources write to the cache on their own schedule. SSE and portfolio code read on their own schedule. No coupling between producer and consumer timing.
- **Thread safety**: The cache uses `threading.Lock` because the Massive client runs via `asyncio.to_thread()` (a real OS thread), not a coroutine.
- **Version counter**: The cache tracks a monotonically-increasing version so SSE can skip sends when nothing has changed.

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py         # Public API re-exports
      models.py           # PriceUpdate dataclass
      cache.py            # PriceCache — thread-safe in-memory store
      interface.py        # MarketDataSource — abstract base class
      seed_prices.py      # Constants: seed prices, GBM params, correlation groups
      simulator.py        # GBMSimulator + SimulatorDataSource
      massive_client.py   # MassiveDataSource
      factory.py          # create_market_data_source()
      stream.py           # FastAPI SSE router
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
```

---

## 3. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only data structure that leaves the market data layer. Every consumer — SSE, portfolio, trade execution — works exclusively with this type.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Design decisions:**

- `frozen=True`: Price updates are immutable value objects, safe to share across async tasks without copying.
- `slots=True`: Minor memory optimization — we create many instances per second.
- Computed properties (`change`, `direction`, `change_percent`): Derived from `price` and `previous_price`, so they can never be stale or inconsistent.
- `to_dict()`: Single serialization point used by both the SSE endpoint and REST API responses.

---

## 4. Price Cache

**File: `backend/app/market/cache.py`**

The central data hub. Data sources write to it; SSE and portfolio routes read from it.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Bumped on every write; enables SSE change detection

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        First update for a ticker sets previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (called on watchlist removal)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Used by SSE for change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why `threading.Lock` and not `asyncio.Lock`?**

The Massive client calls `asyncio.to_thread()` which executes in a real OS thread — `asyncio.Lock` does not protect against that. `threading.Lock` works correctly from both sync threads and the async event loop. CPython's GIL makes `int` reads atomic, but explicit locking is still correct practice and future-proofs against no-GIL Python builds.

**Why a version counter?**

The SSE endpoint polls the cache every 500ms. Without a version counter, it would serialize and send all prices every tick even if nothing changed (Massive API updates every 15s). The version counter enables no-op sends:

```python
last_version = -1
while True:
    current_version = price_cache.version
    if current_version != last_version:
        last_version = current_version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

---

## 5. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for all market data providers.

    Implementations push price updates into a shared PriceCache on their
    own schedule. Downstream code never calls the source directly for
    prices — it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), no further writes happen.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

---

## 6. Seed Data

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no side effects. Shared by the simulator and any other module that needs reference prices.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default 10-ticker watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more movement per tick)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Low vol (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Fallback parameters for dynamically-added tickers not in TICKER_PARAMS
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Sector groups drive the correlation structure in the simulator
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients used in Cholesky decomposition
INTRA_TECH_CORR    = 0.6   # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR   = 0.3   # Across sectors (and unknown tickers)
TSLA_CORR          = 0.3   # TSLA does its own thing even within tech
```

---

## 7. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes with distinct responsibilities:
- `GBMSimulator`: Pure math engine. Stateful — holds current prices and advances them one step at a time.
- `SimulatorDataSource`: The `MarketDataSource` implementation. Wraps `GBMSimulator` in an async loop and writes to `PriceCache`.

### 7.1 GBMSimulator — Math Engine

**The GBM formula:**

```
S(t + dt) = S(t) * exp((mu - sigma²/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` = current price
- `mu` = annualized drift (expected return), e.g. 0.05
- `sigma` = annualized volatility, e.g. 0.22
- `dt` = time step as fraction of a trading year (~8.5×10⁻⁸ for 500ms ticks)
- `Z` = correlated standard normal random variable via Cholesky decomposition

**Why the Itô correction term `(mu - sigma²/2)`?**

Without the correction, GBM would have upward bias because `E[exp(X)] > exp(E[X])` for normal `X`. The Itô term subtracts `sigma²/2 * dt` to keep the expected log return equal to `mu * dt`.

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 s/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% per tick. With 10 tickers at 2 ticks/s,
            # expect an event roughly every 50 seconds — dramatic but not chaotic.
            if random.random() < self._event_prob:
                magnitude = random.uniform(0.02, 0.05)
                sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + magnitude * sign
                logger.debug(
                    "Shock event on %s: %.1f%% %s",
                    ticker,
                    magnitude * 100,
                    "up" if sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — used during batch initialization."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky factor of the correlation matrix.

        Called whenever tickers are added/removed. O(n²) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### 7.2 SimulatorDataSource — Async Wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed the cache before the loop starts so SSE has data on its first tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Key behaviors:**

- **Immediate seeding**: Before the loop starts, `start()` populates the cache with seed prices so SSE has data to send on its very first tick — no blank-screen delay.
- **Graceful cancellation**: `stop()` cancels the task and awaits it, catching `CancelledError`. Ensures clean shutdown during FastAPI lifespan teardown.
- **Exception resilience**: The loop catches exceptions per-step so a single bad tick does not kill the data feed.

---

## 8. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (Polygon.io) REST API snapshot endpoint. The synchronous Massive client runs in `asyncio.to_thread()` to avoid blocking the event loop.

```python
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes to the PriceCache.

    Rate limits:
      Free tier:  5 req/min → poll every 15s (default)
      Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: Any = None

    async def start(self, tickers: list[str]) -> None:
        from massive import RESTClient

        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll so the cache has data before SSE clients connect
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # Synchronous Massive client — run in thread to avoid blocking event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: %d/%d tickers updated", processed, len(self._tickers))
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop retries on next interval.
            # Common causes: 401 (bad key), 429 (rate limit), network error.

    def _fetch_snapshots(self) -> list:
        """Synchronous Massive API call. Always runs inside asyncio.to_thread()."""
        from massive.rest.models import SnapshotMarketType

        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

**Why lazy imports inside `start()` and `_fetch_snapshots()`?**

The `massive` package is only needed when `MASSIVE_API_KEY` is set. Students running the simulator (the default) never need it installed. Moving the import inside the method ensures the simulator path has zero external dependencies beyond `numpy`.

**Error handling philosophy:**

| Error | Behavior |
|-------|----------|
| 401 Unauthorized | Logged as error. Poller continues — key may be fixed and container restarted. |
| 429 Rate Limited | Logged as error. Retries after `poll_interval` seconds. |
| Network timeout | Logged as error. Retries on next cycle. |
| Malformed snapshot | Individual ticker skipped with warning. Others still processed. |
| All tickers fail | Cache retains last-known prices. SSE keeps streaming stale data (better than nothing). |

---

## 9. Factory

**File: `backend/app/market/factory.py`**

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select and return the appropriate market data source.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation, no external deps)

    Returns an unstarted source. Caller must `await source.start(tickers)`.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource

        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)

    from .simulator import SimulatorDataSource

    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

---

## 10. SSE Streaming

**File: `backend/app/market/stream.py`**

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory returning a FastAPI router with the SSE /prices endpoint.

    Factory pattern avoids a module-level global and allows clean injection
    of the PriceCache without relying on app.state in route handlers.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        The client connects with EventSource and receives events in the format:
            data: {"AAPL": {ticker, price, previous_price, ...}, "GOOGL": {...}}

        Pushes all tracked ticker prices every ~500ms.
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Prevent nginx from buffering the stream
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Stops when the client disconnects (detected via request.is_disconnected()).
    Skips sending when the cache version hasn't changed since the last send.
    """
    # Tell the client to retry after 1 second on disconnect
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

**SSE wire format:**

Each event the browser receives looks like:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

**Frontend consumption:**

```javascript
const eventSource = new EventSource('/api/stream/prices');

eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices: { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp } }
    for (const [ticker, update] of Object.entries(prices)) {
        updateTickerDisplay(ticker, update);
    }
};

eventSource.onerror = () => {
    // EventSource auto-reconnects after the retry delay (1000ms)
    setConnectionStatus('reconnecting');
};
```

**Why poll-and-push instead of event-driven?**

Regular 500ms cadence produces evenly-spaced updates for sparkline charts and price flash animations. An event-driven approach would produce bursts (many tickers updated at once by Massive) followed by silence, which would look choppy on the frontend.

---

## 11. Package Init

**File: `backend/app/market/__init__.py`**

```python
"""Market data subsystem public API."""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

All imports from the rest of the backend use `from app.market import ...` — never reaching into submodules.

---

## 12. FastAPI Lifecycle Integration

**File: `backend/app/main.py`** (relevant section)

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---

    # 1. Shared price cache
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Market data source (simulator or Massive, depending on env var)
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load initial watchlist from the database
    initial_tickers = await db.get_watchlist_tickers()  # returns list[str]
    await source.start(initial_tickers)

    # 4. Register SSE route (done here so it has access to price_cache)
    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# Dependency helpers — used by route handlers
def get_price_cache() -> PriceCache:
    return app.state.price_cache

def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

**Accessing market data from route handlers:**

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")

@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(400, f"No price available for {trade.ticker}. Please wait a moment.")
    # ... execute trade at current_price ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.insert_watchlist_entry(payload.ticker)
    await source.add_ticker(payload.ticker)
    return {"ticker": payload.ticker, "status": "added"}


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"ticker": ticker, "status": "removed"}
```

---

## 13. Watchlist Coordination

Whenever the watchlist changes — via REST API or AI chat auto-execution — the data source must be notified.

### Adding a Ticker

```
User/LLM → POST /api/watchlist {ticker: "PYPL"}
  → Insert into watchlist table
  → await source.add_ticker("PYPL")
      Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
      Massive:   appends to ticker list, appears on next poll (~0–15s)
  → Return {ticker: "PYPL", price: <current if available>}
```

### Removing a Ticker

```
User/LLM → DELETE /api/watchlist/PYPL
  → Delete from watchlist table
  → Check if user holds a position in PYPL
      If no position: await source.remove_ticker("PYPL")  (removes from cache too)
      If has position: leave the source tracking it (portfolio valuation needs it)
  → Return {ticker: "PYPL", status: "removed"}
```

### Edge Case: Open Position on Removed Watchlist Ticker

If the user removes a ticker from the watchlist but holds shares, keep tracking it for portfolio valuation:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, ...):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"status": "ok"}
```

---

## 14. Error Handling & Edge Cases

### Empty Watchlist at Startup

If the database has no watchlist entries, `start()` receives `[]`. Both sources handle this gracefully — the simulator produces no prices, Massive skips the API call. SSE sends empty events. When the user adds a ticker, the source starts tracking it immediately.

### Price Cache Miss During Trade

The simulator avoids this by seeding the cache during `add_ticker()`. The Massive client may have a gap between when a ticker is added and the next poll. Return a clear HTTP 400:

```python
price = price_cache.get_price(trade.ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {trade.ticker}. Please wait a moment and try again.",
    )
```

### Invalid Massive API Key

The first poll fails with a 401. The poller logs the error and keeps retrying on the configured interval. The SSE endpoint streams whatever is in the cache (empty at startup). The user sees no prices — a clear indication to check the API key and restart.

### Cholesky Decomposition Failure

If a dynamically-built correlation matrix is not positive semi-definite (shouldn't happen with the defined structure, but possible with unusual tickers), `np.linalg.cholesky` raises `LinAlgError`. Wrap `_rebuild_cholesky` to fall back to no correlation:

```python
def _rebuild_cholesky(self) -> None:
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None
        return
    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = rho
            corr[j, i] = rho
    try:
        self._cholesky = np.linalg.cholesky(corr)
    except np.linalg.LinAlgError:
        logger.warning("Correlation matrix not positive definite; disabling correlations")
        self._cholesky = None
```

---

## 15. Testing

**Test location:** `backend/tests/market/`

**Run with:** `cd backend && uv run pytest tests/market/ -v`

### 15.1 Models

```python
# backend/tests/market/test_models.py
from app.market.models import PriceUpdate


def test_direction_up():
    u = PriceUpdate(ticker="AAPL", price=191.0, previous_price=190.0)
    assert u.direction == "up"
    assert u.change == 1.0
    assert u.change_percent > 0

def test_direction_down():
    u = PriceUpdate(ticker="AAPL", price=189.0, previous_price=190.0)
    assert u.direction == "down"

def test_direction_flat():
    u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=190.0)
    assert u.direction == "flat"
    assert u.change == 0.0

def test_to_dict_keys():
    u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=189.0)
    d = u.to_dict()
    assert set(d.keys()) == {"ticker", "price", "previous_price", "timestamp", "change", "change_percent", "direction"}

def test_immutable():
    import pytest
    u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=189.0)
    with pytest.raises((AttributeError, TypeError)):
        u.price = 200.0  # type: ignore
```

### 15.2 Price Cache

```python
# backend/tests/market/test_cache.py
from app.market.cache import PriceCache


def test_update_and_get():
    cache = PriceCache()
    update = cache.update("AAPL", 190.50)
    assert update.ticker == "AAPL"
    assert update.price == 190.50
    assert cache.get("AAPL") == update

def test_first_update_is_flat():
    cache = PriceCache()
    update = cache.update("AAPL", 190.50)
    assert update.direction == "flat"
    assert update.previous_price == 190.50

def test_subsequent_update_tracks_direction():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    up = cache.update("AAPL", 191.00)
    assert up.direction == "up"
    assert up.previous_price == 190.00

def test_remove():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    cache.remove("AAPL")
    assert cache.get("AAPL") is None

def test_version_increments():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    assert cache.version == v0 + 1

def test_get_all_returns_copy():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    all1 = cache.get_all()
    cache.update("GOOGL", 175.00)
    all2 = cache.get_all()
    assert "GOOGL" not in all1
    assert "GOOGL" in all2

def test_thread_safety():
    import threading
    cache = PriceCache()
    errors = []

    def writer():
        for i in range(500):
            try:
                cache.update("AAPL", float(100 + i))
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
```

### 15.3 GBM Simulator

```python
# backend/tests/market/test_simulator.py
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


def test_step_returns_all_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    assert set(sim.step().keys()) == {"AAPL", "GOOGL"}

def test_prices_always_positive():
    sim = GBMSimulator(tickers=["AAPL"])
    for _ in range(10_000):
        prices = sim.step()
        assert prices["AAPL"] > 0

def test_initial_prices_match_seeds():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

def test_add_ticker_appears_in_step():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("TSLA")
    assert "TSLA" in sim.step()

def test_remove_ticker_absent_from_step():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    sim.remove_ticker("GOOGL")
    assert "GOOGL" not in sim.step()

def test_add_duplicate_is_noop():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("AAPL")
    assert len(sim.get_tickers()) == 1

def test_remove_nonexistent_is_noop():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.remove_ticker("ZZZZ")  # must not raise

def test_empty_step_returns_empty_dict():
    sim = GBMSimulator(tickers=[])
    assert sim.step() == {}

def test_cholesky_built_for_two_tickers():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim._cholesky is None
    sim.add_ticker("GOOGL")
    assert sim._cholesky is not None

def test_full_default_watchlist():
    """Cholesky decomposition succeeds for all 10 default tickers."""
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]
    sim = GBMSimulator(tickers=tickers)
    result = sim.step()
    assert len(result) == 10
    assert all(v > 0 for v in result.values())
```

### 15.4 SimulatorDataSource Integration

```python
# backend/tests/market/test_simulator_source.py
import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:

    async def test_start_seeds_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])
        # Seed prices are in the cache before the first loop tick
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None
        await source.stop()

    async def test_prices_change_over_time(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])
        v0 = cache.version
        await asyncio.sleep(0.4)
        assert cache.version > v0
        await source.stop()

    async def test_stop_is_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()  # Must not raise

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

        await source.stop()
```

### 15.5 Factory

```python
# backend/tests/market/test_factory.py
import os
from unittest.mock import patch
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.simulator import SimulatorDataSource


def test_no_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("MASSIVE_API_KEY", None)
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_empty_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_whitespace_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_valid_api_key_returns_massive():
    from app.market.massive_client import MassiveDataSource
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "pk_test_key"}):
        source = create_market_data_source(cache)
    assert isinstance(source, MassiveDataSource)
```

### 15.6 MassiveDataSource (Mocked)

```python
# backend/tests/market/test_massive.py
from unittest.mock import MagicMock, patch
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _make_snapshot(ticker: str, price: float, timestamp_ms: int = 1707580800000) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


@pytest.mark.asyncio
class TestMassiveDataSource:

    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=999)
        source._tickers = ["AAPL", "GOOGL"]

        snapshots = [_make_snapshot("AAPL", 190.50), _make_snapshot("GOOGL", 175.25)]
        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=999)
        source._tickers = ["AAPL", "BAD"]

        good = _make_snapshot("AAPL", 190.50)
        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None  # Causes AttributeError when accessing .price

        with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=999)
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
            await source._poll_once()  # Must not raise

        assert cache.get_price("AAPL") is None

    async def test_add_remove_ticker(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=999)
        source._tickers = ["AAPL"]

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()

        cache.update("TSLA", 250.0)  # Simulate a price being in cache
        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

    async def test_timestamp_conversion(self):
        """Massive timestamps are ms; cache should receive seconds."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=999)
        source._tickers = ["AAPL"]

        ts_ms = 1707580800000
        snapshots = [_make_snapshot("AAPL", 190.0, timestamp_ms=ts_ms)]

        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update is not None
        assert abs(update.timestamp - ts_ms / 1000.0) < 0.001
```

---

## 16. Configuration Reference

All tunable parameters and their defaults:

| Parameter | Default | Where set | Description |
|-----------|---------|-----------|-------------|
| `MASSIVE_API_KEY` | `""` (empty) | Environment variable | Non-empty → use Massive API; empty → use simulator |
| Simulator update interval | `0.5s` | `SimulatorDataSource.__init__` | Time between GBM steps |
| Massive poll interval | `15.0s` | `MassiveDataSource.__init__` | Time between API calls (free tier: 15s) |
| GBM event probability | `0.001` | `GBMSimulator.__init__` | Per-ticker per-tick chance of a 2–5% shock |
| GBM `dt` | `~8.5e-8` | `GBMSimulator.DEFAULT_DT` | Time step as fraction of a trading year |
| SSE push interval | `0.5s` | `_generate_events()` `interval` param | How often SSE checks the cache and sends |
| SSE retry directive | `1000ms` | `_generate_events()` hardcoded | Browser EventSource reconnection delay |

### `pyproject.toml` requirements

The following must be present in `backend/pyproject.toml`:

```toml
[project]
dependencies = [
    "fastapi",
    "uvicorn[standard]",
    "numpy",
    "massive>=1.0.0",
    # ... other deps
]

[tool.hatch.build.targets.wheel]
packages = ["app"]
```

The `massive` package is a declared dependency even though it is only imported when `MASSIVE_API_KEY` is set. This avoids `ModuleNotFoundError` surprises if someone sets the env var without running `uv sync`.

"""Pytest configuration and shared test fixtures."""

import asyncio
import os
from typing import AsyncGenerator

import fakeredis.aioredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Force test configuration
os.environ["ENVIRONMENT"] = "testing"
os.environ["USE_FAKEREDIS"] = "true"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"

from sqlalchemy.pool import StaticPool

import app.database as db_module
from app.database import get_db
from app.main import app
from app.models import Base
from app.queue.redis_queue import RedisQueueBroker


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def test_engine() -> AsyncGenerator[AsyncEngine, None]:
    """Create an isolated in-memory SQLite async engine per test."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    old_engine = db_module._engine
    old_sessionmaker = db_module._sessionmaker
    db_module._engine = engine
    db_module._sessionmaker = session_factory

    yield engine

    db_module._engine = old_engine
    db_module._sessionmaker = old_sessionmaker
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def db_session(test_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Provide an isolated database session with auto-rollback."""
    session_factory = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def test_broker() -> AsyncGenerator[RedisQueueBroker, None]:
    """Provide a fresh in-memory FakeRedis broker for each test."""
    fake_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    broker = RedisQueueBroker(client=fake_client)
    broker._is_fake = True

    # Monkeypatch the global broker singleton
    import app.queue.redis_queue as rq_module

    old_broker = rq_module._broker
    rq_module._broker = broker

    yield broker

    await fake_client.flushall()
    await fake_client.aclose()
    rq_module._broker = old_broker


@pytest_asyncio.fixture(scope="function")
async def async_client(
    test_engine: AsyncEngine,
    test_broker: RedisQueueBroker,
) -> AsyncGenerator[AsyncClient, None]:
    """Provide an HTTPX AsyncClient wired to the test database and broker."""
    session_factory = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()

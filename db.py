import shutil
import time
from decimal import Decimal, getcontext

# Enough precision to handle extreme decimal values without losing digits
getcontext().prec = 500
from pathlib import Path

from sqlalchemy import (
    Float,
    Index,
    Integer,
    String,
    TypeDecorator,
    create_engine,
    delete,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    mapped_column,
    sessionmaker,
)


class DecimalString(TypeDecorator):
    """Store Decimal as TEXT, return Decimal on read."""
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return str(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return Decimal(str(value))
        return value


class Base(DeclarativeBase):
    pass


class Transaction(Base):
    __tablename__ = "transactions"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id = mapped_column(String, nullable=False)
    from_user_id = mapped_column(String, nullable=False)
    to_user_id = mapped_column(String, nullable=False)
    points = mapped_column(DecimalString, nullable=False)
    reason = mapped_column(String, nullable=True)
    created_at = mapped_column(Float, nullable=False)

    __table_args__ = (
        Index("ix_guild_to_user", "guild_id", "to_user_id"),
    )


engine = create_engine("sqlite:///points.db")
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(engine)


def add_transaction(
    guild_id: str,
    from_user_id: str,
    to_user_id: str,
    points: Decimal,
    reason: str | None,
) -> Transaction:
    with SessionLocal() as session:
        txn = Transaction(
            guild_id=guild_id,
            from_user_id=from_user_id,
            to_user_id=to_user_id,
            points=points,
            reason=reason,
            created_at=time.time(),
        )
        session.add(txn)
        session.commit()
        session.refresh(txn)
        return txn


def get_user_points(guild_id: str, user_id: str) -> Decimal:
    with SessionLocal() as session:
        rows = (
            session.query(Transaction.points)
            .filter(
                Transaction.guild_id == guild_id,
                Transaction.to_user_id == user_id,
            )
            .all()
        )
        return sum((r[0] for r in rows), Decimal("0"))


def get_leaderboard(guild_id: str, limit: int = 10) -> list[tuple[str, Decimal]]:
    with SessionLocal() as session:
        rows = (
            session.query(Transaction.to_user_id, Transaction.points)
            .filter(Transaction.guild_id == guild_id)
            .all()
        )
    # Aggregate in Python for exact Decimal arithmetic
    totals: dict[str, Decimal] = {}
    for user_id, pts in rows:
        totals[user_id] = totals.get(user_id, Decimal("0")) + pts
    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return ranked[:limit]


DB_PATH = Path("points.db")
BACKUP_DIR = Path("backups")


def wipe_guild(guild_id: str) -> tuple[int, str]:
    """Delete all transactions for a guild, backing up the DB first.

    Returns (row_count, backup_filename).
    """
    BACKUP_DIR.mkdir(exist_ok=True)
    backup_name = f"points_{int(time.time())}.db"
    backup_path = BACKUP_DIR / backup_name
    shutil.copy2(DB_PATH, backup_path)

    with SessionLocal() as session:
        result = session.execute(
            delete(Transaction).where(Transaction.guild_id == guild_id)
        )
        session.commit()
        return result.rowcount, backup_name


def get_last_zero_time(guild_id: str, from_user_id: str) -> float | None:
    """Return the timestamp of the user's most recent x0 transaction, or None."""
    with SessionLocal() as session:
        result = (
            session.query(Transaction.created_at)
            .filter(
                Transaction.guild_id == guild_id,
                Transaction.from_user_id == from_user_id,
                Transaction.reason.like("x0%"),
            )
            .order_by(Transaction.created_at.desc())
            .first()
        )
        return result[0] if result else None


def get_user_history(
    guild_id: str, user_id: str, limit: int = 10
) -> list[Transaction]:
    with SessionLocal() as session:
        rows = (
            session.query(Transaction)
            .filter(
                Transaction.guild_id == guild_id,
                Transaction.to_user_id == user_id,
            )
            .order_by(Transaction.created_at.desc())
            .limit(limit)
            .all()
        )
        # Detach from session so they can be used after close
        session.expunge_all()
        return rows

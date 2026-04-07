import shutil
import time
from decimal import Decimal, InvalidOperation, getcontext
from fractions import Fraction

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


Points = Decimal | Fraction


class DecimalString(TypeDecorator):
    """Store Decimal/Fraction as TEXT, return appropriate type on read."""
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return str(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            s = str(value)
            if '/' in s:
                return Fraction(s)
            return Decimal(s)
        return value


def _to_decimal(val: Points) -> Decimal:
    """Convert any point value to Decimal."""
    if isinstance(val, Decimal):
        return val
    return Decimal(val.numerator) / Decimal(val.denominator)


def _add_points(a: Points, b: Points) -> Points:
    """Add two point values, handling Fraction/Decimal mix."""
    if isinstance(a, Decimal) and (a.is_nan() or a.is_infinite()):
        return a + _to_decimal(b)
    if isinstance(b, Decimal) and (b.is_nan() or b.is_infinite()):
        return _to_decimal(a) + b
    # Both finite — use Fraction if either is Fraction for exact arithmetic
    if isinstance(a, Fraction) or isinstance(b, Fraction):
        fa = a if isinstance(a, Fraction) else Fraction(a)
        fb = b if isinstance(b, Fraction) else Fraction(b)
        return fa + fb
    return a + b


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


def get_user_points(guild_id: str, user_id: str) -> Points:
    with SessionLocal() as session:
        rows = (
            session.query(Transaction.points)
            .filter(
                Transaction.guild_id == guild_id,
                Transaction.to_user_id == user_id,
            )
            .all()
        )
        total: Points = Fraction(0)
        for (pts,) in rows:
            try:
                total = _add_points(total, pts)
            except InvalidOperation:
                return Decimal("NaN")
        return total


def get_leaderboard(guild_id: str, limit: int = 10) -> list[tuple[str, Decimal]]:
    with SessionLocal() as session:
        rows = (
            session.query(Transaction.to_user_id, Transaction.points)
            .filter(Transaction.guild_id == guild_id)
            .all()
        )
    # Aggregate in Python for exact arithmetic
    totals: dict[str, Points] = {}
    for user_id, pts in rows:
        try:
            totals[user_id] = _add_points(totals.get(user_id, Fraction(0)), pts)
        except InvalidOperation:
            totals[user_id] = Decimal("NaN")

    def _sort_key(item: tuple[str, Points]) -> tuple[int, Fraction]:
        val = item[1]
        if isinstance(val, Decimal):
            if val.is_nan():
                return (-3, Fraction(0))
            if val.is_infinite():
                return (2, Fraction(0)) if not val.is_signed() else (-2, Fraction(0))
            return (1, Fraction(val))
        return (1, val)

    ranked = sorted(totals.items(), key=_sort_key, reverse=True)
    return ranked[:limit]


DB_PATH = Path("points.db")
BACKUP_DIR = Path("backups")
_CLEAN_DENOM_LIMIT = 1000


def cleanup_float_artifacts() -> int:
    """One-time cleanup: snap float-precision artifacts to clean fractions.

    Backs up the DB first. Returns the number of rows fixed.
    """
    BACKUP_DIR.mkdir(exist_ok=True)
    backup_name = f"points_pre_cleanup_{int(time.time())}.db"
    shutil.copy2(DB_PATH, BACKUP_DIR / backup_name)

    fixed = 0
    with SessionLocal() as session:
        rows = session.query(Transaction).all()
        for txn in rows:
            val = txn.points
            if isinstance(val, Decimal):
                if val.is_nan() or val.is_infinite():
                    continue
                frac = Fraction(val).limit_denominator(_CLEAN_DENOM_LIMIT)
                if frac != Fraction(val):
                    txn.points = frac if frac.denominator != 1 else Decimal(frac.numerator)
                    fixed += 1
            elif isinstance(val, Fraction):
                if val.denominator <= _CLEAN_DENOM_LIMIT:
                    continue
                clean = val.limit_denominator(_CLEAN_DENOM_LIMIT)
                txn.points = clean if clean.denominator != 1 else Decimal(clean.numerator)
                fixed += 1
        session.commit()
    return fixed


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


def get_last_nan_give_time(guild_id: str, from_user_id: str) -> float | None:
    """Return the timestamp of the user's most recent NaN give, or None."""
    with SessionLocal() as session:
        result = (
            session.query(Transaction.created_at)
            .filter(
                Transaction.guild_id == guild_id,
                Transaction.from_user_id == from_user_id,
                Transaction.points.like("%NaN%"),
            )
            .order_by(Transaction.created_at.desc())
            .first()
        )
        return result[0] if result else None


def is_user_nan(guild_id: str, user_id: str) -> bool:
    """Check if a user has any NaN transactions."""
    with SessionLocal() as session:
        return (
            session.query(Transaction)
            .filter(
                Transaction.guild_id == guild_id,
                Transaction.to_user_id == user_id,
                Transaction.points.like("%NaN%"),
            )
            .first()
        ) is not None


def delete_nan_transactions(guild_id: str, user_id: str) -> int:
    """Delete all NaN transactions for a user. Returns count deleted."""
    with SessionLocal() as session:
        result = session.execute(
            delete(Transaction).where(
                Transaction.guild_id == guild_id,
                Transaction.to_user_id == user_id,
                Transaction.points.like("%NaN%"),
            )
        )
        session.commit()
        return result.rowcount


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

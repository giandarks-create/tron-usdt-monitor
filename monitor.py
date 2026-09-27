#!/usr/bin/env python3
"""Poll a TRON wallet for USDT TRC-20 transfers and notify Telegram."""

from __future__ import annotations

import json
import logging
import os
import re
import hashlib
import html
import sys
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv


USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_TRC20_CONTRACT_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13"
DEFAULT_API_URL = "https://apilist.tronscanapi.com/api/token_trc20/transfers"
DEFAULT_ACCOUNT_API_URL = "https://apilist.tronscanapi.com/api/account"
DEFAULT_POLL_SECONDS = 30
DEFAULT_CHECKPOINT_SECONDS = 30 * 60
DEFAULT_PAGE_SIZE = 50
MAX_SEEN_IDS = 2000
NO_DATA = "No disponible"
DOMINICAN_TIMEZONE = ZoneInfo("America/Santo_Domingo")

logger = logging.getLogger("tron-usdt-monitor")
TRON_ADDRESS_PATTERN = re.compile(r"^T[1-9A-HJ-NP-Za-km-z]{33}$")


@dataclass(frozen=True)
class Wallet:
    name: str
    address: str


@dataclass(frozen=True)
class Settings:
    wallets: tuple[Wallet, ...]
    telegram_bot_token: str
    telegram_chat_id: str
    api_url: str = DEFAULT_API_URL
    api_key: str = ""
    poll_seconds: int = DEFAULT_POLL_SECONDS
    page_size: int = DEFAULT_PAGE_SIZE
    state_file: Path = Path("data/seen-transfers.json")
    state_directory: Path = Path("data/seen-transfers")
    dry_run: bool = False
    notify_existing: bool = False

    @classmethod
    def from_environment(cls) -> "Settings":
        required = {
            "TRON_WALLET_ADDRESS": os.getenv("TRON_WALLET_ADDRESS", "").strip(),
            "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(
                "Missing required configuration: "
                + ", ".join(missing)
                + ". Copy .env.example to .env and fill these values."
            )

        try:
            poll_seconds = max(5, int(os.getenv("POLL_SECONDS", str(DEFAULT_POLL_SECONDS))))
            page_size = min(200, max(1, int(os.getenv("PAGE_SIZE", str(DEFAULT_PAGE_SIZE)))))
        except ValueError as exc:
            raise ValueError("POLL_SECONDS and PAGE_SIZE must be whole numbers.") from exc

        legacy_wallet = Wallet(name="Wallet Patricio", address=required["TRON_WALLET_ADDRESS"])
        wallets = load_wallets(
            Path(os.getenv("WALLETS_FILE", "wallets.json")),
            legacy_wallet,
        )
        return cls(
            wallets=wallets,
            telegram_bot_token=required["TELEGRAM_BOT_TOKEN"],
            telegram_chat_id=required["TELEGRAM_CHAT_ID"],
            api_url=os.getenv("TRON_API_URL", DEFAULT_API_URL).strip(),
            api_key=os.getenv("TRON_API_KEY", "").strip(),
            poll_seconds=poll_seconds,
            page_size=page_size,
            state_file=Path(os.getenv("STATE_FILE", "data/seen-transfers.json")),
            state_directory=Path(os.getenv("STATE_DIRECTORY", "data/seen-transfers")),
            dry_run=os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes"},
            notify_existing=os.getenv("NOTIFY_EXISTING", "false").lower()
            in {"1", "true", "yes"},
        )


def load_wallets(path: Path, legacy_wallet: Wallet) -> tuple[Wallet, ...]:
    """Load extra wallets while preserving the existing wallet Secret."""
    try:
        raw_wallets = json.loads(path.read_text())
    except FileNotFoundError:
        raw_wallets = []
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read wallets file {path}: {exc}") from exc

    if not isinstance(raw_wallets, list):
        raise ValueError(f"{path} must contain a JSON array of wallet objects.")

    wallets = [legacy_wallet]
    addresses = {legacy_wallet.address}
    for index, raw_wallet in enumerate(raw_wallets, start=1):
        if not isinstance(raw_wallet, dict):
            raise ValueError(f"Wallet entry {index} must be an object.")
        name = str(raw_wallet.get("name", "")).strip()
        address = str(raw_wallet.get("address", "")).strip()
        if not name or not address:
            raise ValueError(f"Wallet entry {index} needs both name and address.")
        validate_wallet_address(address)
        if address in addresses:
            raise ValueError(f"Wallet entry {index} duplicates an existing address.")
        wallets.append(Wallet(name=name, address=address))
        addresses.add(address)
    return tuple(wallets)


def validate_wallet_address(address: str) -> None:
    """Validate the shape of a mainnet TRON Base58 address locally."""
    if not TRON_ADDRESS_PATTERN.fullmatch(address):
        raise ValueError(
            "Wallet address is not a valid TRON address. "
            "It must be a 34-character Base58 address beginning with T."
        )


def state_path(settings: Settings, wallet: Wallet) -> Path:
    """Keep the original wallet's state path and isolate every new wallet."""
    if wallet.address == settings.wallets[0].address:
        return settings.state_file
    digest = hashlib.sha256(wallet.address.encode("utf-8")).hexdigest()[:16]
    return settings.state_directory / f"{digest}.json"


class SeenTransferStore:
    """Small JSON-backed store so restarts do not resend old notifications."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.ids = self._load()

    def _load(self) -> list[str]:
        try:
            value = json.loads(self.path.read_text())
            if isinstance(value, list):
                return [str(item) for item in value][-MAX_SEEN_IDS:]
        except FileNotFoundError:
            pass
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not read state file %s: %s", self.path, exc)
        return []

    def contains(self, transfer_id: str) -> bool:
        return transfer_id in self.ids

    def add(self, transfer_id: str) -> None:
        if transfer_id not in self.ids:
            self.ids.append(transfer_id)
            self.ids = self.ids[-MAX_SEEN_IDS:]
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps(self.ids, indent=2) + "\n")
            temporary.replace(self.path)


def fetch_transfers(settings: Settings, wallet: Wallet) -> list[dict[str, Any]]:
    headers = {"Accept": "application/json"}
    if settings.api_key:
        headers["TRON-PRO-API-KEY"] = settings.api_key

    params = {
        "relatedAddress": wallet.address,
        "limit": settings.page_size,
        "start": 0,
        "direction": 2,
        "reverse": "true",
        "db_version": 1,
    }
    response = requests.get(settings.api_url, params=params, headers=headers, timeout=20)
    response.raise_for_status()
    payload = response.json()
    transfers = payload.get("token_transfers", payload.get("data", []))
    if not isinstance(transfers, list):
        raise RuntimeError("TRON API returned an unexpected transfer list.")
    return [
        item
        for item in transfers
        if isinstance(item, dict)
        and is_usdt_transfer(item)
        and is_incoming(item, wallet.address)
    ]


def is_usdt_transfer(transfer: dict[str, Any]) -> bool:
    """Match USDT locally because the live endpoint rejects contract_address."""
    candidates = {
        field(transfer, "contract_address", "contractAddress"),
    }
    token_info = transfer.get("tokenInfo")
    if isinstance(token_info, dict):
        candidates.add(str(token_info.get("tokenId", "")))
    return bool(
        {candidate.lower() for candidate in candidates}
        & {USDT_TRC20_CONTRACT.lower(), USDT_TRC20_CONTRACT_HEX.lower()}
    )


def fetch_usdt_balance(settings: Settings, wallet: Wallet) -> Decimal:
    """Read the wallet's USDT balance from TRONSCAN without modifying anything."""
    headers = {"Accept": "application/json"}
    if settings.api_key:
        headers["TRON-PRO-API-KEY"] = settings.api_key
    response = requests.get(
        DEFAULT_ACCOUNT_API_URL,
        params={"address": wallet.address},
        headers=headers,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    balances = payload.get("trc20token_balances", payload.get("balances", []))
    if not isinstance(balances, list):
        raise RuntimeError("TRON API returned an unexpected balance list.")
    for balance in balances:
        if not isinstance(balance, dict):
            continue
        token_id = str(balance.get("tokenId", "")).lower()
        if token_id in {USDT_TRC20_CONTRACT.lower(), USDT_TRC20_CONTRACT_HEX.lower()}:
            raw = str(balance.get("balance", balance.get("amount", "0")))
            decimals = int(balance.get("tokenDecimal", 6))
            return Decimal(raw) / (Decimal(10) ** decimals)
    return Decimal("0")


def transfer_id(transfer: dict[str, Any]) -> str:
    return str(
        transfer.get("transaction_id")
        or transfer.get("transactionHash")
        or transfer.get("hash")
        or transfer.get("id")
        or ""
    )


def field(transfer: dict[str, Any], *names: str) -> str:
    for name in names:
        value = transfer.get(name)
        if value not in (None, ""):
            return str(value)
    return ""


def format_amount(transfer: dict[str, Any]) -> str:
    raw = field(transfer, "quant", "amount_str", "amount")
    token_info = transfer.get("tokenInfo")
    token_decimals = token_info.get("tokenDecimal") if isinstance(token_info, dict) else None
    decimals = str(token_decimals or field(transfer, "decimals") or "6")
    try:
        amount = Decimal(raw) / (Decimal(10) ** int(decimals))
        return format_usdt(amount)
    except (InvalidOperation, ValueError, TypeError):
        return NO_DATA


def is_incoming(transfer: dict[str, Any], wallet_address: str) -> bool:
    return field(transfer, "to_address", "to") == wallet_address


def explorer_url(tx_id: str) -> str:
    return f"https://tronscan.org/#/transaction/{tx_id}"


def format_timestamp(value: str) -> datetime | None:
    if not value.isdigit():
        return None
    try:
        timestamp = datetime.fromtimestamp(
            int(value) / 1000,
            tz=timezone.utc,
        )
        return timestamp
    except (OverflowError, OSError, ValueError):
        return None


def format_local_datetime(value: datetime | None) -> tuple[str, str]:
    if value is None:
        return NO_DATA, NO_DATA
    local_value = value.astimezone(DOMINICAN_TIMEZONE)
    return local_value.strftime("%d/%m/%Y"), local_value.strftime("%-I:%M %p")


def format_usdt(amount: Decimal) -> str:
    formatted = f"{amount:,.6f}".rstrip("0").rstrip(".")
    if "." not in formatted:
        return f"{formatted}.00"
    whole, fraction = formatted.split(".", 1)
    return f"{whole}.{fraction.ljust(2, '0')}"


def format_notification(
    transfer: dict[str, Any],
    wallet: Wallet,
    balance: Decimal | None,
) -> str:
    tx_id = transfer_id(transfer)
    sender = field(transfer, "from_address", "from") or NO_DATA
    recipient = field(transfer, "to_address", "to") or NO_DATA
    timestamp = format_timestamp(field(transfer, "block_ts", "timestamp"))
    amount = format_amount(transfer)
    current_balance = format_usdt(balance) if balance is not None else NO_DATA
    date, clock = format_local_datetime(timestamp)
    safe = lambda value: html.escape(str(value or NO_DATA))
    safe_attr = lambda value: html.escape(str(value or NO_DATA), quote=True)
    tronscan_url = explorer_url(tx_id) if tx_id else ""
    return (
        "🟢 <b>NUEVO INGRESO USDT</b>\n\n"
        f"<b>👤 Wallet:</b> {safe(wallet.name)}\n"
        f"<b>💰 Monto:</b> {safe(amount)} USDT\n"
        f"<b>💵 Saldo actual:</b> {safe(current_balance)} USDT\n\n"
        "<b>📥 Remitente:</b>\n"
        f"<code>{safe(sender)}</code>\n\n"
        "<b>📤 Receptor:</b>\n"
        f"<code>{safe(recipient)}</code>\n\n"
        "<b>🔗 TXID:</b>\n"
        f"<code>{safe(tx_id)}</code>\n\n"
        f"<b>📅 Fecha:</b> {safe(date)}\n"
        f"<b>🕐 Hora:</b> {safe(clock)}\n"
        "🇩🇴 República Dominicana\n"
        "<b>📌 Estado:</b> Nueva transferencia\n\n"
        f'<a href="{safe_attr(tronscan_url)}">🔎 Ver transacción en TRONSCAN</a>\n\n'
        "━━━━━━━━━━━━━━━━━━"
    )


def send_telegram(settings: Settings, message: str, tx_id: str) -> None:
    if settings.dry_run:
        logger.info("DRY_RUN notification:\n%s", message)
        return
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": settings.telegram_chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    response.raise_for_status()
    if not response.json().get("ok", False):
        raise RuntimeError("Telegram rejected the notification.")


def run_once(
    settings: Settings,
    wallet: Wallet,
    store: SeenTransferStore,
    bootstrap: bool = False,
    bootstrap_after_ms: int | None = None,
) -> int:
    transfers = fetch_transfers(settings, wallet)
    fresh = [item for item in transfers if transfer_id(item) and not store.contains(transfer_id(item))]
    fresh.sort(key=lambda item: field(item, "block_ts", "timestamp"))

    sent = 0
    for transfer in fresh:
        tx_id = transfer_id(transfer)
        transfer_timestamp = field(transfer, "block_ts", "timestamp")
        transfer_is_new_since_start = (
            bool(bootstrap_after_ms is not None)
            and transfer_timestamp.isdigit()
            and int(transfer_timestamp) >= bootstrap_after_ms
        )
        if not settings.notify_existing and bootstrap and not transfer_is_new_since_start:
            store.add(tx_id)
            continue
        try:
            balance = fetch_usdt_balance(settings, wallet)
        except (requests.RequestException, RuntimeError, ValueError, InvalidOperation) as exc:
            logger.warning("Could not read current balance for %s: %s", wallet.name, exc)
            balance = None
        send_telegram(settings, format_notification(transfer, wallet, balance), tx_id)
        store.add(tx_id)
        sent += 1
    return sent


def run_checkpoint(
    settings: Settings,
    checked_at: datetime | None = None,
    next_check_at: datetime | None = None,
) -> str:
    """Check every configured wallet without touching transfer state."""
    checked_at = checked_at or datetime.now(timezone.utc)
    next_check_at = next_check_at or (
        checked_at + timedelta(seconds=DEFAULT_CHECKPOINT_SECONDS)
    )

    results: list[tuple[Wallet, Decimal | None, str | None]] = []
    for wallet in settings.wallets:
        try:
            balance = fetch_usdt_balance(settings, wallet)
        except (requests.RequestException, RuntimeError, ValueError, InvalidOperation) as exc:
            logger.error("Checkpoint failed for %s: %s", wallet.name, exc)
            results.append((wallet, None, str(exc).strip() or NO_DATA))
        else:
            results.append((wallet, balance, None))

    verified = sum(balance is not None for _, balance, _ in results)
    total = len(results)
    overall = "OPERATIVO" if verified == total else "ATENCIÓN"
    tron_status = "🟢 Conectado" if verified else "🔴 Error de conexión"
    usdt_status = "🟢 OK" if verified else "🔴 ERROR"
    checked_date, checked_clock = format_local_datetime(checked_at)
    next_date, next_clock = format_local_datetime(next_check_at)
    safe = lambda value: html.escape(str(value or NO_DATA))

    balance_lines = []
    for wallet, balance, error in results:
        if error:
            detail = safe(error[:160])
            balance_lines.append(
                f"• <b>{safe(wallet.name)}</b> — 🔴 ERROR DE CONSULTA\n"
                f"  <i>{detail}</i>"
            )
        else:
            balance_lines.append(
                f"• <b>{safe(wallet.name)}</b>\n"
                f"  {safe(format_usdt(balance or Decimal('0')))} USDT"
            )

    return (
        f"{'🟢' if overall == 'OPERATIVO' else '🔴'} "
        "<b>CHECKPOINT DEL MONITOR</b>\n\n"
        f"<b>Estado general:</b> {overall}\n\n"
        "💰 <b>Saldos actuales</b>\n"
        + "\n".join(balance_lines)
        + "\n\n━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>TRON:</b> {tron_status}\n"
        f"🪙 <b>USDT TRC-20:</b> {usdt_status}\n"
        f"👛 <b>Wallets verificadas:</b> {verified}/{total}\n\n"
        f"🕐 <b>Última comprobación:</b> {checked_date} — {checked_clock}\n"
        "🇩🇴 República Dominicana\n"
        f"🔄 <b>Próxima comprobación:</b> {next_date} — {next_clock}"
    )


def checkpoint_loop(settings: Settings) -> None:
    """Run health checks independently from the 30-second transfer poller."""
    next_run = time.monotonic() + DEFAULT_CHECKPOINT_SECONDS
    while True:
        time.sleep(max(0, next_run - time.monotonic()))
        checked_at = datetime.now(timezone.utc)
        next_check_at = checked_at + timedelta(seconds=DEFAULT_CHECKPOINT_SECONDS)
        try:
            message = run_checkpoint(settings, checked_at, next_check_at)
            send_telegram(settings, message, "checkpoint")
            logger.info(
                "Checkpoint sent for %d configured wallet(s).",
                len(settings.wallets),
            )
        except (requests.RequestException, RuntimeError, OSError, ValueError) as exc:
            logger.error("Checkpoint notification failed: %s", exc)
        finally:
            next_run += DEFAULT_CHECKPOINT_SECONDS


def main() -> int:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        settings = Settings.from_environment()
        for wallet in settings.wallets:
            validate_wallet_address(wallet.address)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2

    if "--check" in sys.argv:
        logger.info(
            "Configuration is valid. Required environment variables are present; "
            "%d wallet(s) configured; wallet address formats are valid; "
            "dry-run is %s. No network requests made.",
            len(settings.wallets),
            settings.dry_run,
        )
        return 0

    logger.info(
        "Monitoring %s every %ss. Dry run: %s",
        len(settings.wallets),
        settings.poll_seconds,
        settings.dry_run,
    )
    threading.Thread(
        target=checkpoint_loop,
        args=(settings,),
        daemon=True,
        name="tron-usdt-checkpoint",
    ).start()
    logger.info(
        "Checkpoint scheduled every %ss independently from transfer polling.",
        DEFAULT_CHECKPOINT_SECONDS,
    )
    monitor_started_at_ms = int(time.time() * 1000)
    while True:
        for wallet in settings.wallets:
            try:
                store = SeenTransferStore(state_path(settings, wallet))
                sent = run_once(
                    settings,
                    wallet,
                    store,
                    bootstrap=not bool(store.ids),
                    bootstrap_after_ms=monitor_started_at_ms,
                )
                if sent:
                    logger.info("Sent %d notification(s) for %s.", sent, wallet.name)
            except requests.RequestException as exc:
                logger.error("Network error for %s; retrying: %s", wallet.name, exc)
            except (RuntimeError, OSError, ValueError) as exc:
                logger.error("Monitor cycle failed for %s; retrying: %s", wallet.name, exc)
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    sys.exit(main())

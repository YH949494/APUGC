import asyncio
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from telegram import Update, InputFile
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ugc_bot")

UTC = timezone.utc

# ---------- ENV ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.getenv("MONGO_DB", "ap")

# drop ids (existing voucher_drops in APreferralV1)
T1_DROP_ID = os.environ["T1_DROP_ID"]   # $1 pool drop_id
T2_DROP_ID = os.environ["T2_DROP_ID"]   # $5 pool drop_id

# optional: validate UGC codes issued by community bot
UGC_CODE_COLLECTION = os.getenv("UGC_CODE_COLLECTION", "ugc_codes")

# where to write ledger (must match APreferralV1 /claim expectation)
REWARD_LEDGER_COLLECTION = os.getenv("REWARD_LEDGER_COLLECTION", "reward_ledger")

# collections for this bot
UGC_SUBMISSIONS_COLLECTION = os.getenv("UGC_SUBMISSIONS_COLLECTION", "ugc_submissions")

# simple anti-spam
MAX_SUBMISSIONS_PER_DAY = int(os.getenv("MAX_SUBMISSIONS_PER_DAY", "20"))

# ---------- Mongo ----------
mongo = MongoClient(MONGO_URI)
db = mongo[MONGO_DB]
ugc_col = db[UGC_SUBMISSIONS_COLLECTION]
code_col = db[UGC_CODE_COLLECTION]
ledger_col = db[REWARD_LEDGER_COLLECTION]


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def normalize_url(url: str) -> str:
    url = url.strip()
    url = re.sub(r"#.*$", "", url)  # remove fragments
    return url


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_str(s: str) -> str:
    return sha256_bytes(s.encode("utf-8"))


def detect_platform(url: str) -> Optional[str]:
    u = url.lower()
    if "tiktok.com" in u:
        return "tt"
    if "instagram.com" in u:
        return "ig"
    if "facebook.com" in u or "fb.watch" in u:
        return "fb"
    return None


def ensure_indexes():
    # UGC dedupe: one post can only be rewarded once
    ugc_col.create_index(
        [("platform", 1), ("post_hash", 1)],
        unique=True,
        name="uq_ugc_platform_posthash"
    )
    ugc_col.create_index([("user_id", 1), ("created_at", -1)], name="ix_ugc_user_created")
    ugc_col.create_index([("status", 1)], name="ix_ugc_status")

    # reward ledger idempotency: one UGC per tier only once
    ledger_col.create_index(
        [("ugc_id", 1), ("tier", 1)],
        unique=True,
        name="uq_reward_ugc_tier"
    )
    ledger_col.create_index([("user_id", 1), ("status", 1), ("claimable_after", 1)], name="ix_reward_user_pending")


# ---------- Conversation states ----------
S_PLATFORM_URL, S_CODE, S_CAPTION, S_PROOF, S_TIER, S_METRICS_PROOF = range(6)


@dataclass
class SubmissionDraft:
    platform: str
    post_url: str
    ugc_code: str
    caption: str
    proof_sha256: str
    tier_claimed: str


async def must_dm(update: Update) -> bool:
    if update.effective_chat and update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text("DM me to submit/claim UGC proof. (Open this bot in private chat.)")
        return False
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await must_dm(update):
        return
    await update.message.reply_text(
        "UGC Proof Bot\n\n"
        "Commands:\n"
        "/submit - submit a post for T1/T2\n"
        "/metrics <ugc_id> - attach metrics proof for T2\n"
        "/status <ugc_id> - check status\n"
    )


def _daily_submission_count(user_id: int) -> int:
    start = now_utc() - timedelta(days=1)
    return ugc_col.count_documents({"user_id": user_id, "created_at": {"$gte": start}})


def validate_code(user_id: int, code: str) -> Tuple[bool, str]:
    code = code.strip()
    doc = code_col.find_one({"code": code})
    if not doc:
        return False, "Code not found. Get a code from the community bot first."
    if int(doc.get("user_id", -1)) != int(user_id):
        return False, "This code is not bound to your account."
    if doc.get("status") in ("used", "expired"):
        return False, "Code already used/expired. Get a new one from the community bot."
    return True, "OK"


async def submit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await must_dm(update):
        return ConversationHandler.END

    user_id = update.effective_user.id
    if _daily_submission_count(user_id) >= MAX_SUBMISSIONS_PER_DAY:
        await update.message.reply_text("Daily submission limit reached. Try again tomorrow.")
        return ConversationHandler.END

    await update.message.reply_text("Send your post URL (FB/IG/TikTok).")
    return S_PLATFORM_URL


async def submit_got_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = normalize_url(update.message.text or "")
    platform = detect_platform(url)
    if not platform:
        await update.message.reply_text("Invalid URL. Must be Facebook / Instagram / TikTok link. Send again.")
        return S_PLATFORM_URL
    context.user_data["platform"] = platform
    context.user_data["post_url"] = url
    context.user_data["post_hash"] = sha256_str(f"{platform}:{url.lower()}")
    await update.message.reply_text("Send your UGC code (from community bot).")
    return S_CODE


async def submit_got_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = (update.message.text or "").strip()
    ok, msg = validate_code(update.effective_user.id, code)
    if not ok:
        await update.message.reply_text(f"{msg}\n\nSend a valid UGC code.")
        return S_CODE
    context.user_data["ugc_code"] = code
    await update.message.reply_text("Send your caption text (copy/paste what you posted).")
    return S_CAPTION


async def submit_got_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caption = (update.message.text or "").strip()
    if len(caption) < 5:
        await update.message.reply_text("Caption too short. Send again.")
        return S_CAPTION
    context.user_data["caption"] = caption
    await update.message.reply_text("Upload ONE screenshot proof of the post (photo).")
    return S_PROOF


async def submit_got_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await update.message.reply_text("Please upload as a photo (not text). Try again.")
        return S_PROOF

    photo = update.message.photo[-1]
    tg_file = await photo.get_file()
    b = await tg_file.download_as_bytearray()
    proof_hash = sha256_bytes(bytes(b))

    context.user_data["proof_sha256"] = proof_hash

    await update.message.reply_text("Which tier are you claiming?\nReply with: T1 or T2")
    return S_TIER


async def submit_got_tier(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tier = (update.message.text or "").strip().upper()
    if tier not in ("T1", "T2"):
        await update.message.reply_text("Invalid. Reply with T1 or T2.")
        return S_TIER

    user_id = update.effective_user.id
    platform = context.user_data["platform"]
    post_url = context.user_data["post_url"]
    post_hash = context.user_data["post_hash"]
    ugc_code = context.user_data["ugc_code"]
    caption = context.user_data["caption"]
    proof_sha256 = context.user_data["proof_sha256"]

    # Create submission
    submission = {
        "user_id": user_id,
        "usernameLower": (update.effective_user.username or "").lower(),
        "platform": platform,
        "post_url": post_url,
        "post_hash": post_hash,
        "ugc_code": ugc_code,
        "caption": caption,
        "proof_sha256": proof_sha256,
        "tier_claimed": tier,
        "status": "submitted",
        "created_at": now_utc(),
        "updated_at": now_utc(),
        "metrics_proof_sha256": None,
        "validated_at": None,
    }

    try:
        res = ugc_col.insert_one(submission)
        ugc_id = str(res.inserted_id)
    except DuplicateKeyError:
        existing = ugc_col.find_one({"platform": platform, "post_hash": post_hash})
        ugc_id = str(existing["_id"])
        await update.message.reply_text(
            f"This post was already submitted.\nUGC ID: {ugc_id}\n"
            f"Use /status {ugc_id}"
        )
        return ConversationHandler.END

    # Mark code used immediately (one code = one post). Community bot can enforce too.
    code_col.update_one({"code": ugc_code}, {"$set": {"status": "used", "used_at": now_utc(), "ugc_id": ugc_id}})

    # T1 auto-validate immediately (MVP) and create reward ledger
    if tier == "T1":
        ugc_col.update_one({"_id": res.inserted_id}, {"$set": {"status": "validated", "validated_at": now_utc()}})
        _create_reward_ledger(user_id=user_id, ugc_id=ugc_id, tier="T1")
        await update.message.reply_text(
            f"✅ Submitted & validated (T1).\nUGC ID: {ugc_id}\n\n"
            f"Claim in community bot: /claim"
        )
        return ConversationHandler.END

    # T2 requires metrics proof
    await update.message.reply_text(
        f"✅ Submitted (T2 pending metrics).\nUGC ID: {ugc_id}\n\n"
        f"Now upload metrics proof:\n/metrics {ugc_id}\n"
        f"(Send a screenshot of views/likes/comments/shares.)"
    )
    return ConversationHandler.END


def _create_reward_ledger(user_id: int, ugc_id: str, tier: str):
    drop_id = T1_DROP_ID if tier == "T1" else T2_DROP_ID
    amount = 1 if tier == "T1" else 5
    doc = {
        "user_id": user_id,
        "ugc_id": ugc_id,
        "tier": tier,
        "amount": amount,
        "drop_id": drop_id,
        "status": "pending_claim",
        "created_at": now_utc(),
        "claimable_after": now_utc(),
        "claimed_at": None,
        "voucher_code": None,
    }
    # idempotent
    ledger_col.update_one(
        {"ugc_id": ugc_id, "tier": tier},
        {"$setOnInsert": doc},
        upsert=True
    )


async def metrics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await must_dm(update):
        return ConversationHandler.END

    if not context.args:
        await update.message.reply_text("Usage: /metrics <ugc_id>")
        return ConversationHandler.END

    ugc_id = context.args[0].strip()
    context.user_data["metrics_ugc_id"] = ugc_id
    await update.message.reply_text("Upload ONE screenshot of the post metrics (photo).")
    return S_METRICS_PROOF


async def metrics_got_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ugc_id = context.user_data.get("metrics_ugc_id")
    if not ugc_id:
        await update.message.reply_text("Missing ugc_id. Use /metrics <ugc_id> again.")
        return ConversationHandler.END

    if not update.message.photo:
        await update.message.reply_text("Please upload as a photo. Try again.")
        return S_METRICS_PROOF

    user_id = update.effective_user.id
    # ensure submission belongs to user
    sub = ugc_col.find_one({"_id": _oid(ugc_id), "user_id": user_id})
    if not sub:
        await update.message.reply_text("UGC not found for your account.")
        return ConversationHandler.END

    photo = update.message.photo[-1]
    tg_file = await photo.get_file()
    b = await tg_file.download_as_bytearray()
    metrics_hash = sha256_bytes(bytes(b))

    # Validate T2 now (MVP: proof-based, no scraping)
    ugc_col.update_one(
        {"_id": _oid(ugc_id)},
        {"$set": {"metrics_proof_sha256": metrics_hash, "status": "validated", "validated_at": now_utc(), "updated_at": now_utc()}}
    )
    _create_reward_ledger(user_id=user_id, ugc_id=ugc_id, tier="T2")

    await update.message.reply_text(
        f"✅ T2 validated.\nUGC ID: {ugc_id}\n\n"
        f"Claim in community bot: /claim"
    )
    return ConversationHandler.END


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await must_dm(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /status <ugc_id>")
        return
    ugc_id = context.args[0].strip()
    sub = ugc_col.find_one({"_id": _oid(ugc_id), "user_id": update.effective_user.id})
    if not sub:
        await update.message.reply_text("Not found.")
        return
    await update.message.reply_text(
        f"UGC {ugc_id}\n"
        f"Platform: {sub.get('platform')}\n"
        f"Tier: {sub.get('tier_claimed')}\n"
        f"Status: {sub.get('status')}\n"
        f"Validated: {sub.get('validated_at')}\n"
        f"Tip: claim rewards in community bot using /claim"
    )


def _oid(s: str):
    # lazy import to avoid dependency if not needed
    from bson import ObjectId
    return ObjectId(s)


def main():
    ensure_indexes()
    app = Application.builder().token(BOT_TOKEN).build()

    submit_conv = ConversationHandler(
        entry_points=[CommandHandler("submit", submit_cmd)],
        states={
            S_PLATFORM_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, submit_got_url)],
            S_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, submit_got_code)],
            S_CAPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, submit_got_caption)],
            S_PROOF: [MessageHandler(filters.PHOTO, submit_got_proof)],
            S_TIER: [MessageHandler(filters.TEXT & ~filters.COMMAND, submit_got_tier)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )

    metrics_conv = ConversationHandler(
        entry_points=[CommandHandler("metrics", metrics_cmd)],
        states={
            S_METRICS_PROOF: [MessageHandler(filters.PHOTO, metrics_got_proof)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(submit_conv)
    app.add_handler(metrics_conv)
    app.add_handler(CommandHandler("status", status_cmd))

    # MVP: polling is simplest on Fly. Webhook can be added later.
    log.info("UGC bot starting (polling)...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
tiktok_obliterator — automated tiktok account banning tool
usage: python main.py --target @username [--intensity maximum]
"""

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

import yaml

# ─── bootstrap logging ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


# ─── CLI ────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        prog="tiktok_obliterator",
        description="automated tiktok account banning via mass reporting",
        epilog="target goes down. you get a ping. that's it.",
    )

    parser.add_argument(
        "--target", "-t",
        required=True,
        help="target tiktok username, @handle, or profile URL",
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "--intensity", "-i",
        choices=["standard", "aggressive", "maximum"],
        default="standard",
        help="campaign intensity (default: standard)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve target and check pool without firing reports",
    )
    parser.add_argument(
        "--pool-only",
        action="store_true",
        help="only create burner accounts, don't target anyone",
    )
    parser.add_argument(
        "--pool-count",
        type=int,
        default=100,
        help="number of burner accounts to pre-create (default: 100)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="show tool statistics and exit",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="enable debug logging",
    )

    return parser.parse_args()


# ─── config loader ──────────────────────────────────────────────────

def load_config(config_path: str) -> dict:
    """load YAML config file, return dict with defaults filled"""
    default_config = {
        "intensity": {
            "standard": {"base_reports_per_wave": 50, "max_escalation": "LEVEL_2"},
            "aggressive": {"base_reports_per_wave": 100, "max_escalation": "LEVEL_3"},
            "maximum": {"base_reports_per_wave": 200, "max_escalation": "LEVEL_4"},
        },
        "monitor": {
            "poll_interval": 30,
            "max_poll_time": 3600,
        },
        "factory": {
            "max_concurrent": 5,
            "email_verify_timeout": 90,
        },
        "report": {
            "max_concurrent": 8,
            "min_delay_ms": 300,
            "max_delay_ms": 8000,
            "max_retries": 2,
        },
        "pool": {
            "min_size": 50,
            "refill_batch": 20,
            "max_reports_per_account": 4,
        },
        "proxies": {
            "file": "proxies/socks5_list.txt",
            "rotation_mode": "random",
            "max_failures": 3,
            "cooldown_seconds": 300,
        },
        "captcha": {
            "provider": "2captcha",
            "api_key": None,
            "fallback_key": None,
            "poll_interval": 3,
            "max_poll_time": 120,
            "max_retries": 3,
            "proxy_type": "socks5",
        },
        "email": {
            "catchall_domain": None,
            "imap_host": None,
            "imap_port": 993,
            "imap_username": None,
            "imap_password": None,
        },
        "notifications": {
            "telegram_bot_token": None,
            "telegram_chat_id": None,
            "discord_webhook_url": None,
        },
        "database": {
            "path": "output/tiktok_obliterator.db",
        },
        "output": {
            "dir": "output",
        },
    }

    config = default_config.copy()

    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file, "r") as f:
            user_config = yaml.safe_load(f) or {}
        # deep merge (simple — top level only)
        for section, values in user_config.items():
            if section in config and isinstance(config[section], dict) and isinstance(values, dict):
                config[section].update(values)
            else:
                config[section] = values
        logger.info("config loaded from %s", config_path)
    else:
        logger.warning("config file not found at %s — using defaults", config_path)
        # write default config so user can edit
        config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(config_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        logger.info("default config written to %s", config_path)

    return config


# ─── dependency builder ─────────────────────────────────────────────

async def build_tool(config: dict, intensity: str):
    """
    wire up all modules based on config
    returns dict of all initialized components
    """
    from core.target_resolver import TargetResolver
    from core.account_factory import AccountFactory
    from core.report_orchestrator import ReportOrchestrator
    from core.ban_monitor import BanMonitor
    from core.escalation_engine import EscalationEngine
    from captcha.solver import CaptchaSolver
    from email.catchall_generator import CatchallGenerator
    from email.verification_listener import VerificationListener
    from network.proxy_rotator import ProxyRotator
    from network.fingerprint_engine import FingerprintEngine
    from network.session_manager import SessionManager
    from storage.db import Database
    from storage.account_pool import AccountPool
    from storage.report_logger import ReportLogger
    from notifications.alerts import AlertManager

    components = {}

    # database
    db = Database(config["database"]["path"])
    await db.initialize()
    components["db"] = db

    # alerts
    alerts = AlertManager(
        telegram_bot_token=config["notifications"]["telegram_bot_token"],
        telegram_chat_id=config["notifications"]["telegram_chat_id"],
        discord_webhook_url=config["notifications"]["discord_webhook_url"],
    )
    await alerts.start()
    components["alerts"] = alerts

    # report logger
    report_logger = ReportLogger(output_dir=config["output"]["dir"])
    components["report_logger"] = report_logger

    # proxy rotator
    proxy_rotator = ProxyRotator(
        proxy_file=config["proxies"]["file"],
        rotation_mode=config["proxies"]["rotation_mode"],
        max_failures=config["proxies"]["max_failures"],
        cooldown_seconds=config["proxies"]["cooldown_seconds"],
    )
    await proxy_rotator.load_proxies()
    components["proxy_rotator"] = proxy_rotator

    # fingerprint engine
    fingerprint_engine = FingerprintEngine()
    components["fingerprint_engine"] = fingerprint_engine

    # session manager
    session_manager = SessionManager()
    await session_manager.start()
    components["session_manager"] = session_manager

    # captcha solver
    if config["captcha"]["api_key"]:
        captcha_solver = CaptchaSolver(
            api_key=config["captcha"]["api_key"],
            fallback_key=config["captcha"]["fallback_key"],
            poll_interval=config["captcha"]["poll_interval"],
            max_poll_time=config["captcha"]["max_poll_time"],
            max_retries=config["captcha"]["max_retries"],
            proxy_type=config["captcha"]["proxy_type"],
        )
        components["captcha"] = captcha_solver
    else:
        logger.warning("no captcha API key configured — account creation will fail")
        components["captcha"] = None

    # email
    email_generator = None
    email_verifier = None
    if config["email"]["catchall_domain"]:
        email_generator = CatchallGenerator(domain=config["email"]["catchall_domain"])
        components["email_generator"] = email_generator

    if all([config["email"]["imap_host"], config["email"]["imap_username"], config["email"]["imap_password"]]):
        email_verifier = VerificationListener(
            imap_host=config["email"]["imap_host"],
            imap_port=config["email"]["imap_port"],
            username=config["email"]["imap_username"],
            password=config["email"]["imap_password"],
        )
        await email_verifier.connect()
        components["email_verifier"] = email_verifier

    # account pool
    account_pool = AccountPool(
        db=db,
        min_pool_size=config["pool"]["min_size"],
        refill_batch_size=config["pool"]["refill_batch"],
        max_reports_per_account=config["pool"]["max_reports_per_account"],
    )
    components["account_pool"] = account_pool

    # account factory
    account_factory = AccountFactory(
        email_domain=config["email"]["catchall_domain"] or "catchall.local",
        captcha_solver=components.get("captcha"),
        proxy_rotator=proxy_rotator,
        fingerprint_engine=fingerprint_engine,
        session_manager=session_manager,
        email_generator=email_generator,
        email_verifier=email_verifier,
        db=db,
        account_pool=account_pool,
        config={"factory_max_concurrent": config["factory"]["max_concurrent"],
                "email_verify_timeout": config["factory"]["email_verify_timeout"]},
    )
    account_pool.factory = account_factory
    components["factory"] = account_factory

    # report orchestrator
    report_orchestrator = ReportOrchestrator(
        account_pool=account_pool,
        proxy_rotator=proxy_rotator,
        session_manager=session_manager,
        db=db,
        report_logger=report_logger,
        config={
            "report_max_concurrent": config["report"]["max_concurrent"],
            "report_min_delay_ms": config["report"]["min_delay_ms"],
            "report_max_delay_ms": config["report"]["max_delay_ms"],
            "report_max_retries": config["report"]["max_retries"],
        },
    )
    components["orchestrator"] = report_orchestrator

    # escalation engine
    escalation_engine = EscalationEngine(
        orchestrator=report_orchestrator,
        account_factory=account_factory,
    )
    components["escalation"] = escalation_engine

    # target resolver
    target_resolver = TargetResolver()
    components["resolver"] = target_resolver

    # ban monitor
    ban_monitor = BanMonitor(
        resolver=target_resolver,
        escalation_engine=escalation_engine,
        config={
            "monitor_poll_interval": config["monitor"]["poll_interval"],
            "monitor_max_poll_time": config["monitor"]["max_poll_time"],
        },
    )
    components["monitor"] = ban_monitor

    # wire up monitor callbacks for alerts
    async def on_status_change(snapshot):
        pass  # can be expanded

    async def on_ban_confirmed(monitor_result):
        await alerts.ban_confirmed(
            target_username=monitor_result.target_username,
            target_uid=monitor_result.target_uid,
            time_to_ban=monitor_result.time_to_ban_seconds or 0,
            total_reports=monitor_result.total_checks,
        )

    ban_monitor.on_ban_confirmed(on_ban_confirmed)

    # initialize pool
    await account_pool.initialize()

    return components


# ─── main commands ──────────────────────────────────────────────────

async def cmd_stats(components: dict):
    """show tool statistics"""
    db = components["db"]
    pool = components["account_pool"]
    proxy_rotator = components["proxy_rotator"]

    db_stats = await db.get_total_stats()
    pool_stats = await pool.get_stats()
    proxy_stats = await proxy_rotator.get_pool_stats()

    print("\n" + "═" * 50)
    print("  TIKTOK OBLITERATOR — STATISTICS")
    print("═" * 50)
    print(f"  Total Reports Fired:      {db_stats['total_reports_fired']}")
    print(f"  Reports Delivered:        {db_stats['total_reports_delivered']}")
    print(f"  Total Bans Confirmed:     {db_stats['total_bans']}")
    print(f"  Burner Accounts Created:  {db_stats['total_accounts_created']}")
    print(f"  Active Accounts:          {db_stats['active_accounts']}")
    print("-" * 50)
    print(f"  Pool Available:           {pool_stats.available}")
    print(f"  Pool In Use:              {pool_stats.in_use}")
    print(f"  Pool Retired:             {pool_stats.retired}")
    print("-" * 50)
    print(f"  Proxies Available:        {proxy_stats['available']}/{proxy_stats['total']}")
    print(f"  Proxies Active:           {proxy_stats['active']}")
    print(f"  Proxies Cooldown:         {proxy_stats['cooldown']}")
    print(f"  Proxies Dead:             {proxy_stats['dead']}")
    print("═" * 50)

    # recent bans
    recent_bans = await db.get_recent_bans(5)
    if recent_bans:
        print("\n  RECENT BANS:")
        for ban in recent_bans:
            print(f"  ★ @{ban['target_username']} — {ban['banned_at']}")
    print()


async def cmd_pool_only(components: dict, count: int):
    """create burner accounts only"""
    factory = components["factory"]
    pool = components["account_pool"]
    alerts = components["alerts"]

    logger.info("creating %d burner accounts...", count)
    await alerts.send(await _make_alert("info", "Account Factory", f"Creating {count} burner accounts..."))

    accounts = await factory.create_batch(count=count, max_parallel=5)
    for acct in accounts:
        await pool.add(acct)

    stats = await pool.get_stats()
    logger.info("done — %d accounts created, pool: %d available", len(accounts), stats.available)
    await alerts.send(await _make_alert(
        "success", "Pool Ready",
        f"{len(accounts)} accounts created. Pool: {stats.available} available.",
    ))


async def cmd_target(components: dict, target: str, intensity: str, dry_run: bool):
    """full campaign against a target"""
    resolver = components["resolver"]
    pool = components["account_pool"]
    orchestrator = components["orchestrator"]
    monitor = components["monitor"]
    escalation = components["escalation"]
    db = components["db"]
    alerts = components["alerts"]
    config = components.get("_config", {})

    # resolve target
    logger.info("resolving target: %s", target)
    target_info = await resolver.resolve(target)

    if target_info.is_banned:
        logger.info("★ @%s is ALREADY BANNED", target_info.username)
        await alerts.send(await _make_alert(
            "success", "Already Banned",
            f"@{target_info.username} is already banned/suspended.",
            {"uid": target_info.uid},
        ))
        return

    if target_info.is_not_found:
        logger.error("target @%s not found", target_info.username)
        return

    logger.info(
        "target acquired — @%s | uid:%s | followers:%s | verified:%s",
        target_info.username,
        target_info.uid,
        target_info.follower_count,
        target_info.is_verified,
    )

    if dry_run:
        stats = await pool.get_stats()
        logger.info("dry run — would target @%s with %d available accounts", target_info.username, stats.available)
        return

    # create campaign
    campaign_id = await db.create_campaign(target_info.username, target_info.uid)

    # notify
    await alerts.campaign_started(target_info.username, (await pool.get_stats()).available)

    # get intensity config
    intensity_cfg = config.get("intensity", {}).get(intensity, {"base_reports_per_wave": 50})
    base_reports = intensity_cfg.get("base_reports_per_wave", 50)

    # set initial escalation level based on intensity
    from core.escalation_engine import EscalationLevel
    intensity_level_map = {
        "standard": EscalationLevel.LEVEL_0,
        "aggressive": EscalationLevel.LEVEL_1,
        "maximum": EscalationLevel.LEVEL_2,
    }
    await escalation.force_escalation(target_info, intensity_level_map.get(intensity, EscalationLevel.LEVEL_0))

    # fire first wave
    logger.info("firing initial wave — %d reports", base_reports)
    first_wave = await orchestrator.fire_wave(
        target_info=target_info,
        report_count=base_reports,
    )

    await alerts.wave_complete(target_info.username, first_wave)

    # start monitoring (this blocks until ban or timeout)
    logger.info("monitoring @%s — will escalate automatically...", target_info.username)
    result = await monitor.monitor_until_banned(target_info)

    # update campaign
    await db.update_campaign(
        campaign_id,
        total_reports_fired=result.total_checks,
        max_escalation_level=escalation._states.get(target_info.uid, None),
    )

    if result.final_status.value == "perm_banned":
        await db.complete_campaign(campaign_id, ban_confirmed=True)
        await db.save_ban({
            "target_username": target_info.username,
            "target_uid": target_info.uid,
            "campaign_id": campaign_id,
            "total_reports_fired": result.total_checks,
            "time_to_ban_seconds": result.time_to_ban_seconds,
            "escalation_level": str(escalation._states.get(target_info.uid, "")),
        })
        logger.info("★ @%s BANNED SUCCESSFULLY", target_info.username)
    else:
        await db.complete_campaign(campaign_id, ban_confirmed=False)
        logger.warning("campaign ended without confirmed ban — status: %s", result.final_status.value)


# ─── helpers ────────────────────────────────────────────────────────

async def _make_alert(level: str, title: str, message: str, metadata: dict = None):
    from notifications.alerts import Alert, AlertLevel
    level_map = {
        "info": AlertLevel.INFO,
        "success": AlertLevel.SUCCESS,
        "warning": AlertLevel.WARNING,
        "error": AlertLevel.ERROR,
    }
    return Alert(
        level=level_map.get(level, AlertLevel.INFO),
        title=title,
        message=message,
        metadata=metadata,
    )


# ─── main ───────────────────────────────────────────────────────────

async def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # load config
    config = load_config(args.config)

    # build tool
    logger.info("initializing tiktok_obliterator...")
    components = await build_tool(config, args.intensity)
    components["_config"] = config

    # handle shutdown gracefully
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info("shutdown signal received — wrapping up...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if args.stats:
            await cmd_stats(components)
        elif args.pool_only:
            await cmd_pool_only(components, args.pool_count)
        else:
            await cmd_target(components, args.target, args.intensity, args.dry_run)
    except KeyboardInterrupt:
        logger.info("interrupted by user")
    except Exception as e:
        logger.exception("fatal error: %s", e)
        if components.get("alerts"):
            await components["alerts"].error_alert(
                "Fatal Error",
                str(e),
            )
    finally:
        # cleanup
        logger.info("shutting down...")
        if components.get("alerts"):
            await components["alerts"].stop()
        if components.get("session_manager"):
            await components["session_manager"].stop()
        if components.get("db"):
            await components["db"].close()
        if components.get("captcha"):
            await components["captcha"].close()
        if components.get("email_verifier"):
            await components["email_verifier"].disconnect()
        logger.info("goodbye.")


if __name__ == "__main__":
    asyncio.run(main())
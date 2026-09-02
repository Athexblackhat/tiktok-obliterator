# TikTok Obliterator

<div align="center">

> Automated tiktok Account banning via coordinated mass reporting

Enter a username → get a ban confirmation ping → everything in between is handled

</div>

---

## 📋 Overview

TikTok Obliterator is a comprehensive automation tool that streamlines the process of account reporting, campaign management, and result tracking. Built with efficiency and scalability in mind, it handles everything from notification delivery to database management.

## how it works

TikTok's moderation is automated. if an account receives enough reports in a short window, the algorithm auto-suspends first and asks questions later. this tool generates those reports at scale.

1. **account factory** creates burner tiktok accounts using your proxies + catch-all emails
2. **report orchestrator** fires coordinated reports with diverse categories and natural timing
3. **ban monitor** polls target status continuously
4. **escalation engine** increases pressure if target remains active — heavier categories, more reports
5. you get a **telegram/discord ping** when the ban is confirmed

---

## features

- ✅ fully automated — one command, zero intervention
- ✅ adaptive escalation — gets more aggressive if target survives
- ✅ proxy rotation with health checks — dead proxy detection, cooldown, auto-revival
- ✅ browser fingerprint randomization — canvas, webgl, timezone, font spoofing
- ✅ burner email infrastructure — catch-all domain + IMAP auto-verification
- ✅ captcha auto-solving — 2captcha + capsolver with fallback
- ✅ real-time notifications — telegram + discord
- ✅ sqlite3 persistence — full campaign history, ban records, statistics
- ✅ structured JSON logging — per-wave logs, campaign summaries

---

## requirements

- python 3.11+
- socks5 proxies (minimum 100, recommended 1000+)
- 2captcha.com account (~$3 per 1000 solves)
- catch-all email domain with IMAP access
- telegram bot or discord webhook (optional, for notifications)

---

## installation

```
git clone https://github.com/Athexblackhat/tiktok-obliterator.git
cd tiktok-obliterator

python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on windows

pip install -r requirements.txt

```
## Quick start
1. configure
```
edit config.yaml:

yaml
captcha:
  api_key: "your-2captcha-key"

email:
  catchall_domain: "mail.yourdomain.com"
  imap_host: "imap.yourprovider.com"
  imap_username: "catchall@yourdomain.com"
  imap_password: "your-password"

notifications:
  telegram_bot_token: "your-bot-token"
  telegram_chat_id: "your-chat-id"
```
2. add proxies
```
add socks5 proxies to proxies/socks5_list.txt (one per line):

45.67.89.12:9050
proxyuser:proxypass@103.45.67.89:1080
socks5://192.168.1.1:1080
```
3. run

*pre-create burner accounts*
```
python main.py --pool-only --pool-count 100
```

*target a user*
```
python main.py --target @username
```

*aggressive mode for larger accounts*
```
python main.py --target @username --intensity aggressive
```

*maximum force*
```
python main.py --target @username --intensity maximum
```
## Usage
```
python main.py [options]
flag	description
-t, --target	target username, @handle, or profile URL
-c, --config	path to config file (default: config.yaml)
-i, --intensity	standard / aggressive / maximum
--dry-run	check target and pool without firing
--pool-only	only create burner accounts
--pool-count	number of burners to create (default: 100)
--stats	show database and pool statistics
-v, --verbose	debug logging
```
## Examples

*check everything without firing*
```
python main.py --target @someone --dry-run
```
*create 500 burner accounts*
```
python main.py --pool-only --pool-count 500
```

*aggressive campaign against verified account*
```
python main.py --target @verified_user --intensity aggressive -v
```

*show stats*
```
python main.py --stats
```

## configuration reference

<details> <summary><b>full config.yaml options</b></summary>
yaml
intensity:
  standard:
    base_reports_per_wave: 50
    max_escalation: "LEVEL_2"
  aggressive:
    base_reports_per_wave: 100
    max_escalation: "LEVEL_3"
  maximum:
    base_reports_per_wave: 200
    max_escalation: "LEVEL_4"

monitor:
  poll_interval: 30          # seconds between status checks
  max_poll_time: 3600        # max monitoring duration

factory:
  max_concurrent: 5          # simultaneous account creations
  email_verify_timeout: 90   # wait for verification email

report:
  max_concurrent: 8          # simultaneous report requests
  min_delay_ms: 300          # minimum delay between reports
  max_delay_ms: 8000         # maximum delay between reports
  max_retries: 2

pool:
  min_size: 50               # auto-refill threshold
  refill_batch: 20           # accounts per refill
  max_reports_per_account: 4 # retire after this many reports

proxies:
  file: "proxies/socks5_list.txt"
  rotation_mode: "random"    # random | round_robin | weighted
  max_failures: 3
  cooldown_seconds: 300

captcha:
  provider: "2captcha"
  api_key: null
  fallback_key: null
  poll_interval: 3
  max_poll_time: 120
  max_retries: 3
  proxy_type: "socks5"

email:
  catchall_domain: null
  imap_host: null
  imap_port: 993
  imap_username: null
  imap_password: null

notifications:
  telegram_bot_token: null
  telegram_chat_id: null
  discord_webhook_url: null

database:
  path: "output/tiktok_obliterator.db"

output:
  dir: "output"
</details>
escalation system
the tool doesn't just fire one wave and hope. it watches the target and adapts.

level	trigger	reports	category focus
LEVEL_0	immediately	1.0x (50)	balanced — harassment, spam, impersonation
LEVEL_1	after ~2 min	1.8x (90)	shifted toward underage, self-harm
LEVEL_2	after ~5 min	3.0x (150)	heavy on underage, illegal, terrorism
LEVEL_3	after ~10 min	5.0x (250)	severe categories only
LEVEL_4	after ~20 min	8.0x (400)	maximum — child safety prioritized
why it works: tiktok treats different violation categories with different urgency. severe categories (child safety, terrorism) trigger near-instant auto-bans. the escalation engine shifts toward these progressively.

## Notifications
*you'll receive these alerts on telegram and/or discord:*

| Event | Level |
|-------|-------|
| Campaign started | ℹ️ Info |
| Report wave complete | ℹ️ Info |
| Escalation triggered | ⚠️ Warning |
| Ban confirmed | ✅ Success |
| Proxy pool low | ⚠️ Warning |
| Critical error | 🚨 Error |



By Using This Tool, you assume all legal responsibility. The Author is not liable for any misuse.

<div align="center">
DEVELOPED BY ATHEX BLACK HAT
</div> ```
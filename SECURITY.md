# Security Policy

`5minPATH` is a research and measurement tool. It does not provide authenticated trading, wallet management, capital management, or production execution.

## Supported Scope

Security reports should target the current `main` branch.

In scope:

- accidental secret exposure in committed files
- unsafe handling of local files or paths
- vulnerabilities in capture, replay, calibration, shadow, dashboard, or CLI code
- documentation that could cause unsafe operational use

Out of scope:

- financial performance claims
- strategy profitability
- third-party service availability
- Polymarket, exchange, oracle, GitHub, or cloud-provider vulnerabilities
- losses from using this research code as a trading system

## Reporting A Vulnerability

Do not post secrets, private keys, API tokens, wallet material, or exploit details in a public issue.

Preferred reporting path:

1. Use GitHub private vulnerability reporting if it is enabled for the repository.
2. If private reporting is unavailable, email `udplost@proton.me`.
3. If neither path is available, open a public issue titled `Security contact request` without sensitive details.

Please include:

- affected file or command
- reproduction steps
- expected impact
- whether any secret or credential was exposed

## Secrets And Credentials

This repo should not contain:

- private keys
- API keys
- wallet seed phrases
- exchange credentials
- SSH keys
- cloud credentials

If you accidentally commit a secret, rotate or revoke it immediately. Removing it from a later commit is not enough once it has been pushed.

## Research And Trading Disclaimer

This repository is for research and measurement only. It is not financial advice, does not recommend live trading, and is not a production trading system.


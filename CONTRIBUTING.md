# Contributing

PlayOnAir is personal MIT open source maintained by Felipe Lima. Useful patches are welcome.

## Language

American English in code, commits, docs, comments, and PR text.

## Toolchain

- **MSRV:** Rust **1.88** (`rust-version` in the workspace `Cargo.toml`)
- Format: `rustfmt.toml` (width 120, 2-space indent, edition 2024)
- Clippy: workspace deny bar (`-D warnings` in the quality gate)

## Quality gate

From the repository root (with [just](https://github.com/casey/just) installed):

```bash
just check
```

That runs format check, clippy, tests, and supply-chain gates. Prefer `cargo nextest` when available (`just test`). Do not land work that fails format, clippy, or deny/audit/machete.

## Conventions

- Conventional commits: `type(scope): subject` (lowercase subject, no trailing period)
- Branches: `type/description` kebab-case
- Product rules: zero required config; AirPlay 2 only; Chromecast only; no second lossy Cast encode by default — see [AGENTS.md](AGENTS.md) and [VISION.md](VISION.md)
- Architecture forks with more than one valid option → ask before implementing

## Git hygiene

- Never commit secrets or device pairing material
- Do not add `Co-Authored-By` trailers (including AI tooling attribution)
- Do not amend published history

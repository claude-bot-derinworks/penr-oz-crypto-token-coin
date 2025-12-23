# penr-oz-crypto-token-coin

## Project overview

This repository is a Python monorepo skeleton for a mini cryptocurrency project built with
FastAPI-based microservices. It contains shared contracts and service entrypoints only,
with no business logic yet.

## Services

- Wallet service
- Transaction service
- Blockchain service
- Miner service

## Shared contracts

The `shared/` package contains locked contracts (constants and Pydantic models) that are
imported by each service.

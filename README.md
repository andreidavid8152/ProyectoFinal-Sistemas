# Proyecto Integrador 202610 - IntegraHub

Estructura base para la plataforma de integracion del flujo Order-to-Cash.

## Objetivo
- APIs sincronas
- Mensajeria asincrona
- Integracion por archivos
- Analitica (streaming o ETL)
- Seguridad, resiliencia e idempotencia

## Arranque (placeholder)
1. Completar `compose.yml` y `.env.example`.
2. Ejecutar: `docker compose up -d`

## Estructura
- `services/` servicios de negocio y demo portal
- `infra/` infraestructura (broker, DB)
- `contracts/` contratos API y eventos
- `docs/` evidencias y diagramas
- `data/` inbox/processed/errors para integracion por archivos


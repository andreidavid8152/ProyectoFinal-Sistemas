# Matriz de patrones

| Patron | Donde se usa | Por que | Trade-off | Evidencia |
| --- | --- | --- | --- | --- |
| Point-to-Point (cola) | RabbitMQ `integrahub.commands` con colas `q.inventory.reserve` y `q.payment.process` consumidas por Inventory/Payment. | Asegura un solo consumidor por comando y un flujo controlado. | Menos flexible para broadcast; requiere gestionar reintentos. | RabbitMQ UI con colas y consumers; logs de `inventory-service` y `payment-service`. |
| Publish/Subscribe | Exchange `integrahub.events` con bindings `order.#` (Notification) y `order.confirmed/rejected` (Order API). | Desacopla productores y multiples consumidores. | Eventual consistency y posibles duplicados. | RabbitMQ UI con exchange/bindings; logs de `notification-service`. |
| Message Router | `services/message-router` enruta eventos raw a comandos y eventos canonicos. | Centraliza reglas de orquestacion y ruteo. | Punto unico de fallo y mayor acoplamiento logico. | Logs de `message-router` con `processed` y tipos. |
| Message Translator | `normalizeType` y `buildCanonicalEvent` en `message-router` normalizan payloads. | Unifica esquemas de eventos para consumo consistente. | Mantenimiento de mapeos; riesgo de perdida de detalle. | Logs con tipos normalizados; routing keys en RabbitMQ. |
| Dead Letter Channel (DLQ) | DLX `integrahub.dlx` y colas `.dlq` en router, inventory, payment y notifications. | Aisla mensajes poison y permite recuperacion. | Operacion manual y backlog si no se atiende. | RabbitMQ UI mostrando mensajes en DLQ; logs "message moved to dlq". |
| Idempotent Consumer | `message-router` (IdempotencyStore) y DB en inventory/payment (command_id). | Evita reprocesar mensajes en reintentos. | Requiere estado y TTL; posible falsa deduplicacion. | Logs "duplicate ignored"; tablas `inventory_reservations` y `payment_transactions`. |

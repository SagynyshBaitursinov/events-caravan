## TODOs:

1. Introduce Event versioning and upcasting mechanism
2. Build an optional build-in solution for Compromise#1 in README.md
3. Provide optional capability not to send Event payload into message broker, but only reference to be used for fetching
   the event details from Event-store.
4. Add events flow traceability, metrics
5. Make it possible to have @ApplyEvent parameter as unwrapped payload (without Event<?>)
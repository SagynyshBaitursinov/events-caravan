# FAQ

- **Q:** Is AI (LLM) utilized when developing Events-caravan?
    - **A:** Yes, for coding, documenting and brainstorming it was useful. However, its output, especially concerning
      important software components, was carefully read and integrated after analysis. I find staying on top of code
      changes and making both strategic and tactical design decisions to be important for developers and software quality.

- **Q:** How to design Aggregates (DDD)?
    - **A:** EventSourcedEntity as single unit of consistency, which can serve as an Aggregate root. Its state may
      contain other domain entities or value objects; and it may maintain business invariants within itself.

- **Q:** Why isn't there a provided Inbox for deduplicating event-messages based on EventReference?
    - **A:** having an "Inbox" table means that application needs to populate it after processing each message. If it's
      done in a separate transaction, appearance of the Inbox entry after a domain-write is not guaranteed. If saving
      the entry is done in the transaction scope as the domain entity writes, it'll contradict the philosophy of the
      framework: the database would be required to coordinate a transaction spanning several tables.

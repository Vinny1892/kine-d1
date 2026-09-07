# Agent instructions

- Read `docs/implementation-notes.md` and `docs/adr/` before changing the
  MongoDB backend. Both record decisions that are not obvious from the code,
  including alternatives that were tried and rejected for measured reasons.
- Design rationale belongs in `docs/`, not in code comments.
- Never print or commit the contents of `MONGO_URI`. Credentials live in
  `~/.config/kine-mongo/env`, outside the repository.
- Preserve unrelated user changes in the working tree.

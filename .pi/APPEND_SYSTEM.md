# Pi-only startup behavior

- AGENTS.md / CLAUDE.md context files are already loaded by pi; do not spend an initial turn searching for them.
- For repo-orientation tasks (e.g. "get up to speed", "read repository", "understand this codebase"), start with the repo-map docs and source-of-truth markdown files before opportunistic README searches.
- Use progressive disclosure: docs first, code second, generated artifacts/tests last.
- In orientation summaries, return: repo map, key docs, implementation shape, current risks/drift, next options.

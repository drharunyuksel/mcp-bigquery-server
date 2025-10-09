# Repository Guidelines

## Project Structure & Module Organization
- `src/index.ts` is the MCP server entry point that wires CLI parsing to BigQuery access helpers.
- `assets/` stores README imagery and should stay out of the published runtime bundle.
- The TypeScript build emits into `dist/`; treat generated files as disposable and never edit them directly.
- Root level metadata (`package.json`, `tsconfig.json`, `CHANGELOG.md`, `README.md`) controls build outputs, compiler behavior, and release notes.

## Build, Test, and Development Commands
- `npm install` installs dependencies; rerun after modifying `package.json`.
- `npm run build` runs `tsc` and refreshes `dist/index.js` for distribution.
- `npm run dev` launches the compiler in watch mode for rapid iteration.
- `npm run clean` removes `dist/` so the next build starts from a clean slate.
- `node dist/index.js --project-id <project> --location <region>` executes the compiled server manually for smoke tests.

## Coding Style & Naming Conventions
- TypeScript with ES modules and strict types; prefer explicit interfaces over `any` and surface BigQuery responses with typed wrappers.
- Use two space indentation, camelCase for functions and variables, PascalCase for classes, and SCREAMING_SNAKE_CASE for env constant names.
- Keep BigQuery utilities cohesive inside feature-scoped helpers and leave CLI wiring confined to the top section of `src/index.ts`.
- Make all credentials and project identifiers injectable via flags or env vars; never hard code them.

## Testing Guidelines
- No automated suite ships today; add tests under `tests/` and register an `npm test` script when introducing coverage.
- Favor deterministic unit tests around request builders by stubbing BigQuery clients and isolating credential handling logic.
- Name specs `<feature>.spec.ts` or `<feature>.test.ts` and keep fixtures free of real datasets or patient identifiers.

## Commit & Pull Request Guidelines
- Write concise, imperative commit titles (e.g., `feat: add dataset schema cache`) and use conventional prefixes when applicable.
- Reference issues or discussions in the commit body when the change affects released behavior or API surfaces.
- PRs should outline behavior changes, manual verification steps, and any configuration migrations or credential expectations.
- Attach screenshots only for asset or documentation updates and triple check that no secrets appear in diffs.

## Security & Configuration Tips
- Never commit service account keys or raw credentials; rely on local env vars or `--key-file` paths added to `.gitignore`.
- Document new required env vars in `README.md` and ensure defaults fail safely when credentials are missing.

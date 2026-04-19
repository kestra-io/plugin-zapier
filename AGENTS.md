# Kestra Zapier Plugin

## What

- Provides plugin components under `io.kestra.plugin.zapier`.
- Includes classes such as `TriggerZap`.

## Why

- What user problem does this solve? Teams need to plugin zapier for Kestra from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Zapier steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Zapier.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `zapier`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.zapier.Example`

### Project Structure

```
plugin-zapier/
├── src/main/java/io/kestra/plugin/zapier/
├── src/test/java/io/kestra/plugin/zapier/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines

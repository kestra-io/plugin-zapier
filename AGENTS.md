# Kestra Zapier Plugin

## What

- Provides plugin components under `io.kestra.plugin.zapier`.
- Includes classes such as `TriggerZap`.

## Why

- This plugin integrates Kestra with Zapier.
- It provides plugin zapier for Kestra.

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

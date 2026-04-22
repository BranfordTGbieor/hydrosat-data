# Sight PoC Data TODO

Purpose: local tracker for application follow-up work. Do not commit unless explicitly requested.

- [x] Configure Docker Hub release secrets and variables in GitHub
- [x] Confirm the release workflow publishes versioned and `latest` tags correctly
- [x] Decide how the released image tag is handed off to `sight-poc-infra`
- [x] Replace the current demo Dagster job with a more realistic sample data engineering project
- [x] Define a simple ETL path using custom Python extraction/loading code
- [x] Model `raw`, `staging`, and `curated` layers in S3
- [x] Add dbt-based transforms for `staging` and `curated` layers, likely running in-cluster
- [x] Orchestrate the end-to-end ETL and dbt flow through Dagster assets/jobs/schedules/sensors
- [x] Add a richer app README section for Dagster job behavior and failure alerting
- [x] Consider adding linting or static analysis alongside tests
- [ ] Add more edge-case tests around missing raw input, malformed records, dbt command failures, and S3 permission errors
- [ ] Add a small integration-style test for the bundled dbt project beyond the mocked runner path
- [ ] Improve code documentation and comments around storage helpers, dbt handoff, and operational sensors
- [ ] Refine sensor behavior for stale-partition recovery, not just missing-partition recovery
- [ ] Consider moving from job/op-centric orchestration to a fuller asset-based model if it meaningfully improves lineage and observability
- [x] Tighten repo cleanup and polish: lint config, formatting enforcement, and any remaining doc consistency issues
- [x] Re-run CI after Docker Hub integration is wired

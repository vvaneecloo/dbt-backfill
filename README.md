## dbt-incremental-batch: backfilling & working with your dbt models made easy.

This was done before the release 1.9.0 of dbt when the new incremental strategy `microbatch` was released - there is no need to use this anymore.

This project is abandoned since, but could be a great ressource for someone only working with materializations / macros, thus I am letting it stay public.

### The idea

- Create a macro / materialization that would help users work with large incremental tables that would fail a `--full-refresh` thus requiring to divide your `dbt run` in small batches.

- Make backfilling usable via (`merge` / `insert_overwrite` strategy) in to bypass a --full-refresh (expensive, takes time etc.).

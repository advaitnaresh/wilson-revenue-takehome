"""Revenue data-quality pipeline.

Stages, in order: ingest -> qc (raw) -> filter -> clean -> transform -> qc (final) -> results.
See run.py for the orchestrator and README_PIPELINE.md for the design writeup.
"""

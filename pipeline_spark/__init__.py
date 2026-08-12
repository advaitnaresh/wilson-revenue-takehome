"""Spark port of pipeline/, for datasets too large for one machine's memory.

Same seven stages, same contract, same QCReport/severity model — everything
that's pure logic (business rules in pipeline.config, type/key inference in
pipeline.introspect, the QCIssue/QCReport dataclasses in pipeline.qc) is
imported and reused rather than re-written. What's rewritten is only the part
that has to change when the data no longer fits on one machine: every
operation here is a distributed DataFrame transformation, and the driver only
ever collects small aggregates or a bounded sample — never the full table.

See README_BIGDATA.md for the design writeup.
"""

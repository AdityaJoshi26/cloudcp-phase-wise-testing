# CLI dataset notes

This folder mirrors the `CloudCpBinaryTesting/data/` role, but the source datasets for CLI
testing remain authoritative in `dataset_cloudcp/spec_files/`.

Use this folder for:

- Notes about which dataset ids are suitable for CLI smoke, boundary, and regression runs.
- Small helper files specific to CLI-only validation workflows.
- Future curated subsets if you need a hand-maintained shortlist.

Current recommended starting points:

- `DS-P8-02`: smallest practical smoke test.
- `DS-P2-01`: boundary-value probe.
- `DS-P4-01`: filename stress with manageable size.
- `DS-P7-01`: realistic mixed workload.

The runner itself reads directly from `dataset_cloudcp/spec_files/manifest.json` and the
corresponding `DS-P*/` spec directories.
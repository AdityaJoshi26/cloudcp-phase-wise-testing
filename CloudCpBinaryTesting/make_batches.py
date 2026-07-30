#!/usr/bin/env python3
"""Create NUL-framed batch files from a source directory.

Walks a directory tree and writes the file paths it finds into one or more
"batch" files. Each batch file is a stream of raw path bytes where every
record is terminated by a single NUL byte (\\0) -- the same format cloudcp /
BatchBuilder consumes.

Why NUL: file paths can contain spaces, newlines, carriage returns and
trailing whitespace. NUL is the only byte that cannot appear in a POSIX path,
so it is the only safe record delimiter (same idea as `find -print0`).

Negative mode (-n / --negative): datagen cannot produce corrupt / hostile
artifacts, so this mode builds them here -- a tree of hostile filesystem
objects (broken symlinks, unreadable files, FIFOs, weird names) plus a set of
deliberately malformed batch files (bad framing, dangling paths, etc.). These
feed the cloudcp negative test cases (see plan_cp_binary.md).

Usage:
    python make_batches.py /path/to/dir
    python make_batches.py /path/to/dir -o out_dir --batch-size 500
    python make_batches.py /path/to/dir --single        # one file, all paths
    python make_batches.py -n -o CloudcpBinaryTesting    # build negative suite
"""

import argparse
import json
import os
import stat
import sys


def iter_files(root):
    """Yield every regular file path under *root* (bytes-safe, no symlink follow).

    Uses an explicit stack + os.scandir so large trees don't blow the
    recursion limit and so path bytes round-trip unchanged.
    """
    stack = [os.fsencode(root)]
    while stack:
        current = stack.pop()
        try:
            it = os.scandir(os.fsdecode(current))
        except OSError as e:
            print("skip (cannot scandir): {}: {}".format(os.fsdecode(current), e),
                  file=sys.stderr)
            continue
        with it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(os.fsencode(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        yield entry.path
                except OSError as e:
                    print("skip (stat failed): {}: {}".format(entry.path, e),
                          file=sys.stderr)


def write_batch(paths, batch_path):
    """Write *paths* to *batch_path* as a NUL-terminated stream of raw bytes."""
    with open(batch_path, "wb") as bf:
        for p in paths:
            bf.write(os.fsencode(p) + b"\0")


# ---------------------------------------------------------------------------
# Negative / corrupt test-case generation
#
# datagen only produces well-formed data, so the hostile artifacts below are
# built here. POSIX-only objects (symlinks, FIFOs, chmod 000, newline / non-
# UTF-8 names) are created on Linux/macOS and skipped with a notice on Windows,
# since cloudcp itself runs on Linux.
# ---------------------------------------------------------------------------

_POSIX = os.name == "posix"


def _skip(what, reason):
    print("  skip {}: {}".format(what, reason), file=sys.stderr)


def _write_file(path_bytes, size=0, fill=b"\0"):
    """Create a regular file of *size* bytes at *path_bytes* (bytes path)."""
    with open(path_bytes, "wb") as f:
        if size:
            chunk = (fill * 4096)[:4096] if fill else b"\0" * 4096
            written = 0
            while written < size:
                n = min(len(chunk), size - written)
                f.write(chunk[:n])
                written += n
    return path_bytes


def build_negative_files(data_dir):
    """Create hostile filesystem objects under *data_dir*.

    Returns a list of (label, path_bytes) for the objects actually created, so
    the caller can reference them from batch files.
    """
    ddir = os.fsencode(data_dir)
    os.makedirs(ddir, exist_ok=True)
    created = []

    def rec(label, pb):
        created.append((label, pb))

    # N06: zero-byte file (valid edge case).
    rec("N06_zero_byte", _write_file(os.path.join(ddir, b"n06_zero_byte.bin"), 0))

    # A small valid target used by the symlink cases.
    real_target = _write_file(os.path.join(ddir, b"n_real_target.bin"), 1024)
    real_dir = os.path.join(ddir, b"n_real_dir")
    os.makedirs(real_dir, exist_ok=True)

    # N07: embedded spaces in the name.
    rec("N07_spaces", _write_file(os.path.join(ddir, b"n07 file with spaces .bin"), 2048))

    # N10: very long name (~255 bytes, the usual NAME_MAX).
    long_name = b"n10_" + (b"x" * 240) + b".bin"
    try:
        rec("N10_long_name", _write_file(os.path.join(ddir, long_name), 512))
    except OSError as e:
        _skip("N10_long_name", e)

    # N11: very deep path (~PATH_MAX). Build nested dirs then a file.
    try:
        deep = ddir
        for i in range(40):
            deep = os.path.join(deep, b"n11_lvl_%02d_xxxxxxxxxxxxxxxxxxxx" % i)
        os.makedirs(deep, exist_ok=True)
        rec("N11_deep_path", _write_file(os.path.join(deep, b"leaf.bin"), 256))
    except OSError as e:
        _skip("N11_deep_path", e)

    if not _POSIX:
        _skip("N01-N05,N08,N09", "POSIX-only objects, host is not POSIX")
        return created

    # N01: broken symlink (target does not exist).
    try:
        p = os.path.join(ddir, b"n01_broken_symlink.bin")
        if os.path.lexists(p):
            os.remove(p)
        os.symlink(os.path.join(ddir, b"does_not_exist_target.bin"), p)
        rec("N01_broken_symlink", p)
    except OSError as e:
        _skip("N01_broken_symlink", e)

    # N02: symlink to a real file.
    try:
        p = os.path.join(ddir, b"n02_symlink_to_file.bin")
        if os.path.lexists(p):
            os.remove(p)
        os.symlink(real_target, p)
        rec("N02_symlink_to_file", p)
    except OSError as e:
        _skip("N02_symlink_to_file", e)

    # N03: symlink to a directory.
    try:
        p = os.path.join(ddir, b"n03_symlink_to_dir")
        if os.path.lexists(p):
            os.remove(p)
        os.symlink(real_dir, p)
        rec("N03_symlink_to_dir", p)
    except OSError as e:
        _skip("N03_symlink_to_dir", e)

    # N04: unreadable file (chmod 000).
    try:
        p = _write_file(os.path.join(ddir, b"n04_no_read_perm.bin"), 4096)
        os.chmod(p, 0)
        rec("N04_no_read_perm", p)
    except OSError as e:
        _skip("N04_no_read_perm", e)

    # N05: named pipe / FIFO.
    try:
        p = os.path.join(ddir, b"n05_fifo")
        if os.path.lexists(p):
            os.remove(p)
        os.mkfifo(p)
        rec("N05_fifo", p)
    except (OSError, AttributeError) as e:
        _skip("N05_fifo", e)

    # N08: embedded newline / carriage return in the name.
    try:
        rec("N08_newline", _write_file(os.path.join(ddir, b"n08_line1\nline2\rline3.bin"), 1024))
    except OSError as e:
        _skip("N08_newline", e)

    # N09: non-UTF-8 / invalid byte sequence in the name.
    try:
        rec("N09_nonutf8", _write_file(os.path.join(ddir, b"n09_\xff\xfe_bad.bin"), 1024))
    except OSError as e:
        _skip("N09_nonutf8", e)

    return created


def build_negative_batches(batch_dir, data_dir, created):
    """Write deliberately malformed / hostile batch files into *batch_dir*.

    *created* is the list of (label, path_bytes) real objects from
    build_negative_files, referenced where a batch needs valid-but-hostile paths.
    """
    bdir = os.fsencode(batch_dir)
    ddir = os.fsencode(data_dir)
    os.makedirs(bdir, exist_ok=True)

    by_label = dict(created)

    def raw(name, data):
        p = os.path.join(bdir, os.fsencode(name))
        with open(p, "wb") as f:
            f.write(data)
        print("wrote {} ({} bytes)".format(os.fsdecode(p), len(data)))

    # A couple of well-framed real paths to mix into partial-failure batches.
    good = by_label.get("N06_zero_byte") or (created[0][1] if created else ddir)
    good2 = by_label.get("N07_spaces", good)

    # B01: empty batch (0 bytes).
    raw("bad_batch_empty.txt", b"")

    # B02: last record missing its trailing NUL.
    raw("bad_batch_no_terminator.txt", good + b"\0" + good2)

    # B03: two consecutive NULs (empty record in the middle).
    raw("bad_batch_double_nul.txt", good + b"\0\0" + good2 + b"\0")

    # B04: leading NUL (empty first record).
    raw("bad_batch_leading_nul.txt", b"\0" + good + b"\0")

    # B05: nothing but NUL bytes.
    raw("bad_batch_only_nuls.txt", b"\0" * 8)

    # B06: well-framed but every path is dangling (does not exist).
    raw("bad_batch_dangling_paths.txt",
        os.path.join(ddir, b"nope_a.bin") + b"\0" +
        os.path.join(ddir, b"nope_b.bin") + b"\0")

    # B07: a directory path where a file is expected.
    raw("bad_batch_directory_entry.txt", ddir + b"\0")

    # B08: paths containing embedded CR/LF (valid via NUL framing).
    raw("bad_batch_crlf_paths.txt",
        os.path.join(ddir, b"has\r\ncrlf_a.bin") + b"\0" +
        os.path.join(ddir, b"has\ncrlf_b.bin") + b"\0")

    # B09: path with non-UTF-8 / surrogate bytes.
    raw("bad_batch_nonutf8.txt", os.path.join(ddir, b"\xff\xfe_bad.bin") + b"\0")

    # B10: single path far exceeding PATH_MAX.
    raw("bad_batch_very_long_path.txt",
        os.path.join(ddir, b"z" * 5000) + b".bin\0")

    # B11: a record that is only whitespace, then NUL.
    raw("bad_batch_whitespace_only.txt", b"   \t  \0")

    # B12: alternating valid and dangling paths (partial-success contract).
    raw("bad_batch_mixed_valid_invalid.txt",
        good + b"\0" +
        os.path.join(ddir, b"missing_1.bin") + b"\0" +
        good2 + b"\0" +
        os.path.join(ddir, b"missing_2.bin") + b"\0")


def run_negative(output_dir):
    """Build the full negative suite under *output_dir*."""
    data_dir = os.path.join(output_dir, "negative_data")
    batch_dir = os.path.join(output_dir, "negative_batches")
    print("building negative filesystem objects in {} ...".format(data_dir))
    created = build_negative_files(data_dir)
    for label, pb in created:
        print("  created {}: {}".format(label, os.fsdecode(pb)))
    print("building malformed batch files in {} ...".format(batch_dir))
    build_negative_batches(batch_dir, data_dir, created)
    print("negative suite complete: {} objects, batch files in {}".format(
        len(created), batch_dir))
    return 0


# ---------------------------------------------------------------------------
# Scenario B: corrupted batches built from a REAL, materialized dataset.
#
# Unlike --negative (which references hostile / non-existent paths), here every
# valid record points at a file that actually exists on disk. The batch FRAMING
# is corrupted, so cloudcp can start the transfer, upload the valid records that
# precede the corruption, and must then handle the bad framing gracefully.
#
# For each variant we also record the ordered "expected-success prefix": the
# real records that come before the first corruption point (and therefore should
# transfer successfully). A companion corrupt_manifest.json drives validation in
# run_cloudcp_tests.py.
# ---------------------------------------------------------------------------

def _framed(paths):
    """Join *paths* (bytes) into a well-framed NUL-terminated stream."""
    return b"".join(p + b"\0" for p in paths)


def run_corrupt_from(dataset_dir, output_dir):
    """Build corrupted batch variants + manifest from a real dataset dir."""
    ddir = os.fsencode(dataset_dir)
    paths = sorted(os.fsencode(p) for p in iter_files(dataset_dir))  # bytes, deterministic
    n = len(paths)
    if n == 0:
        print("no files found under {}; nothing to corrupt".format(dataset_dir),
              file=sys.stderr)
        return 2

    batches_dir = os.path.join(output_dir, "corrupt_batches")
    os.makedirs(batches_dir, exist_ok=True)

    k = max(1, n // 2)                           # split point for mid-corruptions
    dec = os.fsdecode                            # bytes path -> str for manifest

    cases = []

    def add_case(cid, corruption, data, expected_paths, independent, note):
        fname = "{}.txt".format(cid)
        fpath = os.path.join(batches_dir, fname)
        with open(fpath, "wb") as f:
            f.write(data)
        rel = os.path.join("corrupt_batches", fname)
        cases.append({
            "id": cid,
            "batch": rel,
            "corruption": corruption,
            "expected_success": [dec(p) for p in expected_paths],
            "expected_success_count": len(expected_paths),
            "independent_records": independent,
            "note": note,
        })
        print("wrote {} ({} bytes, expect >={} success)".format(
            fpath, len(data), len(expected_paths)))

    # C01: truncated tail -- first k records complete, then a partial (no NUL).
    tail = paths[k] if k < n else paths[-1]
    add_case(
        "C01_truncated_tail",
        "first {} records well-framed, then a record cut mid-path with no NUL".format(k),
        _framed(paths[:k]) + tail[: max(1, len(tail) // 2)],
        paths[:k], False,
        "records at/after the truncation are lost")

    # C02: last record missing its trailing NUL.
    add_case(
        "C02_no_terminator",
        "all records framed except the last, which is missing its trailing NUL",
        _framed(paths[:-1]) + paths[-1],
        paths[:-1], False,
        "the unterminated final record is best-effort")

    # C03: double NUL (empty record) injected after k real records.
    add_case(
        "C03_double_nul",
        "an empty record (double NUL) injected after {} real records".format(k),
        _framed(paths[:k]) + b"\0" + _framed(paths[k:]),
        paths[:k], False,
        "records after the empty record depend on parser resync")

    # C04: leading NUL (empty first record), then all real records.
    add_case(
        "C04_leading_nul",
        "batch starts with a NUL (empty first record) before any real path",
        b"\0" + _framed(paths),
        [], False,
        "corruption is at position 0; graceful early handling expected")

    # C05: whitespace-only record inserted after k real records.
    add_case(
        "C05_whitespace_record",
        "a whitespace-only record inserted after {} real records".format(k),
        _framed(paths[:k]) + b"   \t  \0" + _framed(paths[k:]),
        paths[:k], False,
        "the whitespace path does not exist and must error, not crash")

    # C06: alternating real + dangling paths (independent-record partial success).
    mixed = bytearray()
    for i, p in enumerate(paths):
        mixed += p + b"\0"
        mixed += os.path.join(ddir, ("__missing_%d.bin" % i).encode()) + b"\0"
    add_case(
        "C06_mixed_valid_dangling",
        "each real record followed by a dangling (non-existent) path",
        bytes(mixed),
        paths, True,
        "records are independent: every real file should upload, danglings fail")

    manifest = {
        "dataset_dir": dec(ddir),
        "fs_prefix": dec(ddir),
        "total_real_files": n,
        "batches_dir": "corrupt_batches",
        "cases": cases,
    }
    manifest_path = os.path.join(output_dir, "corrupt_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print("corrupt suite complete: {} cases from {} real files -> {}".format(
        len(cases), n, manifest_path))
    return 0


def main():
    ap = argparse.ArgumentParser(description="Create NUL-framed batch file(s) from a directory.")
    ap.add_argument("src_dir", nargs="?",
                    help="Directory to walk. Not required with --negative.")
    ap.add_argument("-o", "--output-dir", default="batches",
                    help="Where to write batch files (default: ./batches). "
                         "With --negative, the negative_data/ and negative_batches/ "
                         "subdirs are created under this path.")
    ap.add_argument("--batch-size", type=int, default=1000,
                    help="Max files per batch file (default: 1000). Ignored with --single.")
    ap.add_argument("--single", action="store_true",
                    help="Write all paths into one file (batch_000000.txt).")
    ap.add_argument("-n", "--negative", action="store_true",
                    help="Generate the negative/corrupt test suite (hostile files + "
                         "malformed batch files) instead of walking a directory.")
    ap.add_argument("--corrupt-from", metavar="DATASET_DIR",
                    help="Scenario B: build corrupted batch variants + a manifest "
                         "from an existing, materialized dataset directory (the real "
                         "files must already exist on disk). Output goes under "
                         "-o/--output-dir (corrupt_batches/ + corrupt_manifest.json).")
    args = ap.parse_args()

    if args.negative:
        os.makedirs(args.output_dir, exist_ok=True)
        return run_negative(args.output_dir)

    if args.corrupt_from:
        if not os.path.isdir(args.corrupt_from):
            print("Not a directory: {}".format(args.corrupt_from), file=sys.stderr)
            return 2
        os.makedirs(args.output_dir, exist_ok=True)
        return run_corrupt_from(args.corrupt_from, args.output_dir)

    if not args.src_dir:
        print("src_dir is required unless --negative is given.", file=sys.stderr)
        return 2

    if not os.path.isdir(args.src_dir):
        print("Not a directory: {}".format(args.src_dir), file=sys.stderr)
        return 2

    os.makedirs(args.output_dir, exist_ok=True)

    batch_id = 0
    total = 0
    buf = []

    def flush():
        nonlocal batch_id, buf
        if not buf:
            return
        name = "batch_{:06d}.txt".format(batch_id)
        path = os.path.join(args.output_dir, name)
        write_batch(buf, path)
        print("wrote {} ({} files)".format(path, len(buf)))
        batch_id += 1
        buf = []

    for fpath in iter_files(args.src_dir):
        buf.append(fpath)
        total += 1
        if not args.single and len(buf) >= args.batch_size:
            flush()
    flush()

    print("done: {} files across {} batch file(s)".format(total, max(batch_id, 1)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

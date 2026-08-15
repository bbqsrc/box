# Command-line integration

## Command surface

> [spec:box:req:cli-commands.root]
> The `box` binary MUST require and dispatch exactly one of `create`, `extract`,
> `list`, `info`, or `validate`. It MUST retain the aliases `c`, `x`, `l`/`ls`,
> and `t`/`test`, parse arguments from the platform-aware `wild` iterator, run
> command handlers on the Tokio runtime, and propagate handler failures through
> the CLI diagnostic result.

> [spec:box:req:cli-commands.root.create+1]
> `box create` MUST require at least one input operand, construct the archive
> with the requested alignment and path-policy flags, set its archive-level
> creation timestamp, create parent directories before dependent entries, and
> process directory records before file payloads and internal symlink records
> only after their target file records have stable indices. Checksums and
> parallel file processing MUST be enabled by default; `--no-checksum` and
> `--serial` MAY disable them. Both serial and parallel creation MUST preserve
> requested file attributes, including extended attributes. A successful
> command MUST finish the writer before reporting archive size and file totals.

> [spec:box:req:cli-commands.root.extract]
> `box extract` MUST open the requested archive, create the output directory,
> and pass checksum, escaped-path, external-symlink, and xattr choices to the
> archive extraction API. With no member operands it MUST extract the entire
> archive, sequentially only when `--serial` is set and otherwise with the
> requested or CPU-count concurrency. With member operands it MUST convert each
> operand to a `BoxPath` and recursively extract those selections sequentially.
> Explicit worker counts for parallel commands MUST be positive.

> [spec:box:req:cli-commands.root.inspect]
> `box list` MUST support compact, long, and pretty-printed JSON views and MUST
> distinguish directories, ordinary files, chunked files, internal links, and
> external links. `box info` MUST show either archive-wide version, alignment,
> counts, sizes, attribute keys, and attributes, or record-specific type,
> location, size, compression, link target, block, and typed-attribute data for
> the selected member.

> [spec:box:req:cli-commands.root.validate]
> `box validate` MUST validate all checksummed file content through either the
> sequential validator selected by `--serial` or the parallel validator using
> the requested or CPU-count concurrency. Its result MUST retain separate counts
> for checked files, files without checksums, and checksum failures.

## Input selection

> [spec:box:sem:cli-selection.root]
> Create operands are interpreted from left to right under a mutable compression
> configuration initially set to Zstandard. `--zstd`/`--zstandard`, `--stored`,
> and `--xz` replace that configuration; `-O` consumes the following `key=value`
> operand when present. Each other operand captures the configuration active at
> its position and is classified as a glob when it contains `*`, `?`, or `[`.

> [spec:box:sem:cli-selection.root.collection]
> An explicit regular-file operand contributes that `BoxPath` at most once. A
> directory operand is walked asynchronously; candidates are filtered by hidden
> status, exclusion patterns, the recursion setting, and duplicate path sets,
> then classified as directories, symlinks, or files while retaining the
> compression configuration attached to the originating operand.

## Safety and failure behavior

> [spec:box:req:cli-safety.root]
> Extraction MUST refuse an archive whose header permits escaped paths or
> external symlinks unless the corresponding `--allow-escapes` or
> `--allow-external-symlinks` consent flag is present. Creation MUST reject a
> symlink target that cannot be resolved to an archive record unless external
> symlinks were explicitly enabled; when enabled, it MUST store the raw relative
> target with normalized forward separators.

> [spec:box:req:cli-safety.root.output-integrity]
> Creation MUST NOT overwrite an existing archive without `--force`. Forced
> replacement MUST remove the existing output before opening the new writer,
> and directory collection MUST exclude the canonical path of the archive being
> created so that the archive cannot include itself.

> [spec:box:req:cli-safety.root.failure-status]
> Path, archive-open, filesystem, and extraction failures MUST be returned as
> typed diagnostics. Extraction and validation MUST terminate with a failing
> process status when any checksum fails, even in quiet mode; quiet mode MAY
> suppress progress and summaries but MUST NOT change validation or consent
> decisions.

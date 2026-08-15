# Portable archive paths

> [spec:box:req:paths.root]
> A constructed or fully validated BoxPath MUST be a nonempty relative sequence of nonempty components. It MUST NOT contain `.` or `..` components, leading or trailing archive separators, consecutive archive separators, `/` or `\` inside components, or control characters. Archive path comparison and FST lookup use the BoxPath's UTF-8 bytes rather than its displayed platform spelling.

> [spec:box:def:paths.root.encoding]
> BoxPath is a UTF-8 string whose component separator is U+001F UNIT SEPARATOR. It has no leading or trailing separator. Display and conversion to a platform Path replace U+001F with `/` on Unix-family targets and `\` on Windows; path-FST keys use the original U+001F-separated bytes.

> [spec:box:req:paths.root.normalization]
> Converting a platform path to BoxPath MUST ignore root, platform prefix, and current-directory components; each parent component MUST remove the most recent retained normal component and MUST have no effect when none remains. Each retained component MUST be Unicode, trimmed at both ends, nonempty, NFC-normalized, and free of slash, backslash, controls, and Unicode separators other than ordinary space. Conversion MUST fail if no components remain. Escape-aware construction MAY retain a literal `\xNN` spelling only when it is syntactically complete and its decoded ASCII byte would itself be permitted; ordinary construction MUST reject the backslash.

> [spec:box:req:paths.root.extraction-gates+1]
> Extraction MUST refuse an archive whose escaped-path flag is set unless the
> caller explicitly enables escaped paths, and MUST refuse one whose
> external-symlink flag is set unless the caller explicitly enables external
> symlinks. Before materializing any indexed record or resolving an internal
> link target, extraction MUST validate the unnormalized logical path, require
> complete `\xNN` grammar only when the archive enables escapes, reject unsafe
> components, require an exact path-index mapping when an FST exists, and require
> the final component to equal the record name. Its platform conversion MUST
> remain a relative sequence of normal components, with no root or platform
> prefix. An ExternalLink target is used directly as a platform path after the
> archive-level consent check; it is not normalized as a BoxPath or independently
> confined beneath the extraction directory. Writers adding an ExternalLink MUST
> set the corresponding header flag so this gate is reached.

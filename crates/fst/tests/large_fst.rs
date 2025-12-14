use box_fst::{Fst, FstBuilder};
use rand::distributions::WeightedIndex;
use rand::prelude::*;

const PATH_SEP: char = '\x1f';

/// Realistic filesystem path generator based on statistical models
struct RealisticPathGenerator {
    rng: StdRng,
    /// Common top-level directory names (weighted)
    top_dirs: Vec<(&'static str, u32)>,
    /// Common subdirectory name patterns
    subdir_patterns: Vec<&'static str>,
    /// Common file extensions (weighted by frequency)
    extensions: Vec<(&'static str, u32)>,
    /// Words for generating names
    words: Vec<&'static str>,
    /// Cached distributions
    top_dirs_dist: WeightedIndex<u32>,
    extensions_dist: WeightedIndex<u32>,
}

impl RealisticPathGenerator {
    fn new(seed: u64) -> Self {
        let top_dirs = vec![
            ("src", 30),
            ("lib", 15),
            ("test", 12),
            ("tests", 12),
            ("docs", 8),
            ("examples", 6),
            ("benches", 4),
            ("vendor", 10),
            ("node_modules", 8),
            ("target", 5),
            ("build", 5),
            ("dist", 4),
            (".git", 3),
            ("assets", 4),
            ("static", 3),
            ("public", 3),
            ("config", 4),
            ("scripts", 3),
        ];
        let extensions = vec![
            (".rs", 20),
            (".js", 18),
            (".ts", 15),
            (".py", 12),
            (".go", 8),
            (".java", 6),
            (".json", 10),
            (".toml", 5),
            (".yaml", 5),
            (".yml", 4),
            (".md", 8),
            (".txt", 4),
            (".html", 5),
            (".css", 5),
            (".scss", 3),
            (".xml", 2),
            (".sql", 3),
            (".sh", 2),
            (".lock", 2),
        ];

        let top_dirs_weights: Vec<u32> = top_dirs.iter().map(|(_, w)| *w).collect();
        let extensions_weights: Vec<u32> = extensions.iter().map(|(_, w)| *w).collect();

        Self {
            rng: StdRng::seed_from_u64(seed),
            top_dirs_dist: WeightedIndex::new(&top_dirs_weights).unwrap(),
            extensions_dist: WeightedIndex::new(&extensions_weights).unwrap(),
            top_dirs,
            subdir_patterns: vec![
                "utils",
                "helpers",
                "core",
                "common",
                "shared",
                "internal",
                "api",
                "models",
                "views",
                "controllers",
                "services",
                "components",
                "hooks",
                "stores",
                "actions",
                "reducers",
                "types",
                "interfaces",
                "traits",
                "impls",
                "modules",
                "handlers",
                "middleware",
                "routes",
                "pages",
                "layouts",
                "fixtures",
                "mocks",
                "stubs",
                "snapshots",
                "data",
                "v1",
                "v2",
                "legacy",
                "deprecated",
                "experimental",
            ],
            extensions,
            words: vec![
                "main",
                "index",
                "app",
                "mod",
                "lib",
                "util",
                "helper",
                "config",
                "settings",
                "options",
                "parser",
                "builder",
                "reader",
                "writer",
                "handler",
                "processor",
                "manager",
                "service",
                "client",
                "server",
                "connection",
                "session",
                "request",
                "response",
                "error",
                "result",
                "value",
                "node",
                "tree",
                "list",
                "map",
                "set",
                "queue",
                "stack",
                "cache",
                "store",
                "buffer",
                "stream",
                "channel",
                "test",
                "spec",
                "bench",
                "example",
                "demo",
                "sample",
                "user",
                "auth",
                "login",
                "register",
                "profile",
                "account",
                "data",
                "model",
                "entity",
                "record",
                "row",
                "item",
                "file",
                "path",
                "dir",
                "folder",
                "archive",
                "package",
            ],
        }
    }

    /// Generate depth using exponential decay (most files at depth 2-4)
    fn random_depth(&mut self) -> usize {
        // Exponential distribution biased toward shallow depths
        let r: f64 = self.rng.r#gen();
        let depth = (-r.ln() * 2.0) as usize;
        depth.clamp(1, 10)
    }

    /// Generate a random directory name
    fn random_dir_name(&mut self) -> String {
        if self.rng.r#gen_bool(0.3) {
            // Use a pattern name
            self.subdir_patterns
                .choose(&mut self.rng)
                .unwrap()
                .to_string()
        } else if self.rng.r#gen_bool(0.2) {
            // Use word + number
            let word = *self.words.choose(&mut self.rng).unwrap();
            format!("{}_{}", word, self.rng.r#gen_range(1..100))
        } else {
            // Use a word
            self.words.choose(&mut self.rng).unwrap().to_string()
        }
    }

    /// Generate a random filename
    fn random_filename(&mut self) -> String {
        let ext_idx = self.extensions_dist.sample(&mut self.rng);
        let ext = self.extensions[ext_idx].0;

        let name = if self.rng.r#gen_bool(0.1) {
            // Special files
            *["README", "LICENSE", "Makefile", "Dockerfile", "Cargo"]
                .choose(&mut self.rng)
                .unwrap()
        } else if self.rng.r#gen_bool(0.3) {
            // word_word pattern - need to build string
            let w1 = *self.words.choose(&mut self.rng).unwrap();
            let w2 = *self.words.choose(&mut self.rng).unwrap();
            return format!("{}_{}{}", w1, w2, ext);
        } else if self.rng.r#gen_bool(0.2) {
            // word + number
            let word = *self.words.choose(&mut self.rng).unwrap();
            return format!("{}{}{}", word, self.rng.r#gen_range(1..1000), ext);
        } else {
            // Single word
            *self.words.choose(&mut self.rng).unwrap()
        };

        format!("{}{}", name, ext)
    }

    /// Generate a single random path
    fn generate_path(&mut self) -> String {
        let depth = self.random_depth();
        let mut components = Vec::with_capacity(depth + 1);

        // First component is a top-level dir
        let top_idx = self.top_dirs_dist.sample(&mut self.rng);
        components.push(self.top_dirs[top_idx].0.to_string());

        // Middle components are subdirs
        for _ in 1..depth {
            components.push(self.random_dir_name());
        }

        // Last component is a filename
        components.push(self.random_filename());

        components.join(&PATH_SEP.to_string())
    }

    /// Generate n unique paths, sorted
    fn generate_paths(&mut self, n: usize) -> Vec<(Vec<u8>, u64)> {
        use std::collections::BTreeSet;

        let mut paths = BTreeSet::new();

        // Generate more than needed to account for duplicates
        while paths.len() < n {
            let batch_size = (n - paths.len()) * 2;
            for _ in 0..batch_size {
                paths.insert(self.generate_path());
                if paths.len() >= n {
                    break;
                }
            }
        }

        paths
            .into_iter()
            .take(n)
            .enumerate()
            .map(|(i, p)| (p.into_bytes(), i as u64))
            .collect()
    }
}

#[test]
#[ignore] // Run with: cargo test --release -p box-fst -- --ignored --nocapture
fn test_large_fst_realistic() {
    const TARGET_COUNT: usize = 1_000_000;

    println!("Generating {} realistic paths...", TARGET_COUNT);
    let mut generator = RealisticPathGenerator::new(42);
    let start = std::time::Instant::now();
    let paths = generator.generate_paths(TARGET_COUNT);
    println!("Generated {} paths in {:?}", paths.len(), start.elapsed());

    // Print some sample paths
    println!("\nSample paths:");
    for (path, _) in paths.iter().take(10) {
        println!("  {}", String::from_utf8_lossy(path).replace(PATH_SEP, "/"));
    }
    println!("  ...");
    for (path, _) in paths
        .iter()
        .rev()
        .take(5)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        println!("  {}", String::from_utf8_lossy(path).replace(PATH_SEP, "/"));
    }

    // Analyze path statistics
    let depths: Vec<usize> = paths
        .iter()
        .map(|(p, _)| p.iter().filter(|&&b| b == PATH_SEP as u8).count() + 1)
        .collect();
    let avg_depth = depths.iter().sum::<usize>() as f64 / depths.len() as f64;
    let max_depth = *depths.iter().max().unwrap();
    let avg_len = paths.iter().map(|(p, _)| p.len()).sum::<usize>() as f64 / paths.len() as f64;

    println!("\nPath statistics:");
    println!("  Average depth: {:.2}", avg_depth);
    println!("  Max depth: {}", max_depth);
    println!("  Average path length: {:.1} bytes", avg_len);

    println!("\nBuilding FST...");
    let start = std::time::Instant::now();
    let mut builder = FstBuilder::new();
    for (path, value) in &paths {
        builder.insert(path, *value).unwrap();
    }
    let fst_data = builder.finish().unwrap();
    let build_time = start.elapsed();

    println!("Build time: {:?}", build_time);
    println!(
        "FST size: {} bytes ({:.2} MB)",
        fst_data.len(),
        fst_data.len() as f64 / 1_000_000.0
    );

    let raw_size: usize = paths.iter().map(|(k, _)| k.len() + 8).sum();
    println!(
        "Raw path data size: {} bytes ({:.2} MB)",
        raw_size,
        raw_size as f64 / 1_000_000.0
    );
    println!(
        "Compression ratio: {:.2}x",
        raw_size as f64 / fst_data.len() as f64
    );

    let fst = Fst::new(&fst_data).unwrap();
    assert_eq!(fst.len(), paths.len() as u64);

    println!("\nVerifying all {} lookups...", paths.len());
    let start = std::time::Instant::now();
    for (path, expected_value) in &paths {
        let actual = fst.get(path);
        assert_eq!(
            actual,
            Some(*expected_value),
            "Mismatch for path {:?}",
            String::from_utf8_lossy(path)
        );
    }
    let lookup_time = start.elapsed();
    println!("All lookups verified in {:?}", lookup_time);
    println!(
        "Average lookup time: {:?}",
        lookup_time / paths.len() as u32
    );

    println!("\nAll tests passed!");
}

#[test]
#[ignore]
fn test_large_fst_prefix_queries_realistic() {
    const TARGET_COUNT: usize = 1_000_000;

    println!("Generating {} realistic paths...", TARGET_COUNT);
    let mut generator = RealisticPathGenerator::new(42);
    let paths = generator.generate_paths(TARGET_COUNT);

    println!("Building FST...");
    let mut builder = FstBuilder::new();
    for (path, value) in &paths {
        builder.insert(path, *value).unwrap();
    }
    let fst_data = builder.finish().unwrap();
    let fst = Fst::new(&fst_data).unwrap();

    // Test prefix query for "src" directory
    let prefix = format!("src{}", PATH_SEP).into_bytes();
    println!("\nQuerying prefix: 'src/'");

    let start = std::time::Instant::now();
    let results: Vec<_> = fst.prefix_iter(&prefix).collect();
    let query_time = start.elapsed();

    println!("Found {} entries in {:?}", results.len(), query_time);
    assert!(results.len() > 0, "Expected some results for 'src/' prefix");

    // Print some matching paths
    println!("Sample matches:");
    for (key, _) in results.iter().take(5) {
        println!("  {}", String::from_utf8_lossy(key).replace(PATH_SEP, "/"));
    }

    // Test prefix for "test" directory
    let prefix2 = format!("test{}", PATH_SEP).into_bytes();
    println!("\nQuerying prefix: 'test/'");
    let results2: Vec<_> = fst.prefix_iter(&prefix2).collect();
    println!("Found {} entries", results2.len());

    // Test prefix for "lib" directory
    let prefix3 = format!("lib{}", PATH_SEP).into_bytes();
    println!("\nQuerying prefix: 'lib/'");
    let results3: Vec<_> = fst.prefix_iter(&prefix3).collect();
    println!("Found {} entries", results3.len());

    // Empty prefix (all entries)
    println!("\nQuerying empty prefix (all entries)...");
    let start = std::time::Instant::now();
    let all_count = fst.prefix_iter(b"").count();
    let query_time = start.elapsed();
    println!("Found {} entries in {:?}", all_count, query_time);
    assert_eq!(all_count, paths.len());

    println!("\nAll prefix query tests passed!");
}

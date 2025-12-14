use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::distributions::WeightedIndex;
use rand::prelude::*;

use box_fst::{Fst, FstBuilder};

const PATH_SEP: char = '\x1f';

/// High-fanout path generator for stress testing FST node formats.
/// Generates flat directories with many long-named files.
struct HighFanoutGenerator {
    rng: StdRng,
    /// Prefixes that create diverse first-bytes (A-Z coverage)
    prefixes: Vec<&'static str>,
    /// Domain nouns for realistic names
    nouns: Vec<&'static str>,
    /// Action words
    actions: Vec<&'static str>,
    /// Suffixes
    suffixes: Vec<&'static str>,
    /// Extensions
    extensions: Vec<&'static str>,
}

impl HighFanoutGenerator {
    fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            // Prefixes covering A-Z for maximum first-byte diversity
            prefixes: vec![
                "Abstract",
                "Admin",
                "Analytics",
                "Application",
                "Async",
                "Base",
                "Batch",
                "Binary",
                "Bound",
                "Buffer",
                "Cache",
                "Client",
                "Cloud",
                "Cluster",
                "Config",
                "Data",
                "Database",
                "Debug",
                "Default",
                "Dependency",
                "Editor",
                "Email",
                "Encryption",
                "Entity",
                "Environment",
                "Factory",
                "Feature",
                "File",
                "Filter",
                "Format",
                "Gateway",
                "Generic",
                "Global",
                "Graph",
                "Grid",
                "Handler",
                "Hash",
                "Header",
                "Health",
                "Http",
                "Identity",
                "Image",
                "Import",
                "Index",
                "Input",
                "Job",
                "Json",
                "Jwt",
                "Kafka",
                "Kernel",
                "Key",
                "Layout",
                "Lazy",
                "Legacy",
                "List",
                "Loader",
                "Manager",
                "Mapper",
                "Memory",
                "Message",
                "Metrics",
                "Native",
                "Network",
                "Node",
                "Notification",
                "Null",
                "Object",
                "Observer",
                "Operation",
                "Optional",
                "Output",
                "Package",
                "Parser",
                "Payload",
                "Permission",
                "Pipeline",
                "Query",
                "Queue",
                "Rate",
                "Reader",
                "Redis",
                "Registry",
                "Remote",
                "Schema",
                "Security",
                "Serializer",
                "Service",
                "Session",
                "Table",
                "Task",
                "Template",
                "Thread",
                "Token",
                "Upload",
                "User",
                "Utility",
                "Validation",
                "Value",
                "Version",
                "View",
                "Virtual",
                "Webhook",
                "Widget",
                "Window",
                "Worker",
                "Wrapper",
                "Xml",
                "Yaml",
                "Zero",
                "Zone",
            ],
            nouns: vec![
                "Authentication",
                "Authorization",
                "Configuration",
                "Connection",
                "Controller",
                "Coordinator",
                "Dashboard",
                "Dispatcher",
                "Execution",
                "Expression",
                "Implementation",
                "Initialization",
                "Integration",
                "Interceptor",
                "Management",
                "Middleware",
                "Navigation",
                "Notification",
                "Orchestration",
                "Persistence",
                "Processing",
                "Provisioning",
                "Registration",
                "Repository",
                "Resolution",
                "Scheduling",
                "Serialization",
                "Synchronization",
                "Transformation",
                "Translation",
                "Validation",
                "Verification",
            ],
            actions: vec![
                "Builder",
                "Checker",
                "Converter",
                "Creator",
                "Decorator",
                "Delegate",
                "Encoder",
                "Executor",
                "Extractor",
                "Fetcher",
                "Formatter",
                "Generator",
                "Handler",
                "Helper",
                "Importer",
                "Inspector",
                "Invoker",
                "Iterator",
                "Listener",
                "Locator",
                "Monitor",
                "Normalizer",
                "Optimizer",
                "Organizer",
                "Parser",
                "Processor",
                "Producer",
                "Provider",
                "Publisher",
                "Resolver",
                "Scanner",
                "Selector",
                "Transformer",
                "Updater",
                "Validator",
            ],
            suffixes: vec![
                "Component",
                "Container",
                "Context",
                "Dialog",
                "Directive",
                "Element",
                "Factory",
                "Guard",
                "Hook",
                "Interceptor",
                "Manager",
                "Modal",
                "Module",
                "Panel",
                "Pipe",
                "Plugin",
                "Popup",
                "Portal",
                "Provider",
                "Reducer",
                "Resolver",
                "Router",
                "Selector",
                "Service",
                "Store",
                "Strategy",
                "System",
                "Template",
                "Toolbar",
                "Tracker",
                "Utility",
                "View",
                "Widget",
                "Wizard",
                "Wrapper",
            ],
            extensions: vec![".ts", ".tsx", ".js", ".jsx", ".rs", ".go", ".py"],
        }
    }

    /// Generate a single long filename like "UserAuthenticationHandlerComponent.tsx"
    fn generate_filename(&mut self) -> String {
        let prefix = self.prefixes.choose(&mut self.rng).unwrap();
        let noun = self.nouns.choose(&mut self.rng).unwrap();
        let action = self.actions.choose(&mut self.rng).unwrap();
        let suffix = self.suffixes.choose(&mut self.rng).unwrap();
        let ext = self.extensions.choose(&mut self.rng).unwrap();
        format!("{}{}{}{}{}", prefix, noun, action, suffix, ext)
    }

    /// Generate N unique paths in a flat directory
    fn generate_flat_dir(&mut self, n: usize, dir: &str) -> Vec<(Vec<u8>, u64)> {
        use std::collections::BTreeSet;
        let mut paths = BTreeSet::new();
        while paths.len() < n {
            let filename = self.generate_filename();
            let path = format!("{}{}{}", dir, PATH_SEP, filename);
            paths.insert(path);
        }
        paths
            .into_iter()
            .enumerate()
            .map(|(i, p)| (p.into_bytes(), i as u64))
            .collect()
    }

    /// Generate paths across multiple directories, each with high fanout
    fn generate_multi_dir(&mut self, files_per_dir: usize, dirs: &[&str]) -> Vec<(Vec<u8>, u64)> {
        use std::collections::BTreeSet;
        let mut all_paths = BTreeSet::new();
        for dir in dirs {
            let mut count = 0;
            while count < files_per_dir {
                let filename = self.generate_filename();
                let path = format!("{}{}{}", dir, PATH_SEP, filename);
                if all_paths.insert(path) {
                    count += 1;
                }
            }
        }
        all_paths
            .into_iter()
            .enumerate()
            .map(|(i, p)| (p.into_bytes(), i as u64))
            .collect()
    }
}

/// Realistic filesystem path generator based on statistical models
struct RealisticPathGenerator {
    rng: StdRng,
    top_dirs: Vec<(&'static str, u32)>,
    subdir_patterns: Vec<&'static str>,
    extensions: Vec<(&'static str, u32)>,
    words: Vec<&'static str>,
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
            (".json", 10),
            (".toml", 5),
            (".yaml", 5),
            (".md", 8),
            (".txt", 4),
            (".html", 5),
            (".css", 5),
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
                "types",
                "modules",
                "handlers",
                "middleware",
                "routes",
                "pages",
                "data",
            ],
            extensions,
            words: vec![
                "main", "index", "app", "mod", "lib", "util", "helper", "config", "parser",
                "builder", "reader", "writer", "handler", "service", "client", "server", "request",
                "response", "error", "node", "tree", "list", "map", "cache", "store", "buffer",
                "test", "spec", "user", "auth", "data", "model", "file",
            ],
        }
    }

    fn random_depth(&mut self) -> usize {
        let r: f64 = self.rng.r#gen();
        ((-r.ln() * 2.0) as usize).clamp(1, 10)
    }

    fn random_dir_name(&mut self) -> String {
        if self.rng.r#gen_bool(0.3) {
            self.subdir_patterns
                .choose(&mut self.rng)
                .unwrap()
                .to_string()
        } else if self.rng.r#gen_bool(0.2) {
            let word = *self.words.choose(&mut self.rng).unwrap();
            format!("{}_{}", word, self.rng.r#gen_range(1..100))
        } else {
            self.words.choose(&mut self.rng).unwrap().to_string()
        }
    }

    fn random_filename(&mut self) -> String {
        let ext_idx = self.extensions_dist.sample(&mut self.rng);
        let ext = self.extensions[ext_idx].0;

        if self.rng.r#gen_bool(0.3) {
            let w1 = *self.words.choose(&mut self.rng).unwrap();
            let w2 = *self.words.choose(&mut self.rng).unwrap();
            format!("{}_{}{}", w1, w2, ext)
        } else if self.rng.r#gen_bool(0.2) {
            let word = *self.words.choose(&mut self.rng).unwrap();
            format!("{}{}{}", word, self.rng.r#gen_range(1..1000), ext)
        } else {
            let word = *self.words.choose(&mut self.rng).unwrap();
            format!("{}{}", word, ext)
        }
    }

    fn generate_path(&mut self) -> String {
        let depth = self.random_depth();
        let mut components = Vec::with_capacity(depth + 1);

        let top_idx = self.top_dirs_dist.sample(&mut self.rng);
        components.push(self.top_dirs[top_idx].0.to_string());

        for _ in 1..depth {
            components.push(self.random_dir_name());
        }
        components.push(self.random_filename());
        components.join(&PATH_SEP.to_string())
    }

    fn generate_paths(&mut self, n: usize) -> Vec<(Vec<u8>, u64)> {
        use std::collections::BTreeSet;
        let mut paths = BTreeSet::new();
        while paths.len() < n {
            paths.insert(self.generate_path());
        }
        paths
            .into_iter()
            .enumerate()
            .map(|(i, p)| (p.into_bytes(), i as u64))
            .collect()
    }
}

fn generate_paths(n: usize) -> Vec<(Vec<u8>, u64)> {
    RealisticPathGenerator::new(42).generate_paths(n)
}

fn build_fst(paths: &[(Vec<u8>, u64)]) -> Vec<u8> {
    let mut builder = FstBuilder::new();
    for (path, value) in paths {
        builder.insert(path, *value).unwrap();
    }
    builder.finish().unwrap()
}

fn build_hashmap(paths: &[(Vec<u8>, u64)]) -> HashMap<Vec<u8>, u64> {
    paths.iter().cloned().collect()
}

fn bench_single_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_lookup");

    for size in [10_000, 100_000, 1_000_000, 10_000_000] {
        let paths = generate_paths(size);
        let fst_data = build_fst(&paths);
        let fst = Fst::new(&fst_data).unwrap();
        let hashmap = build_hashmap(&paths);

        // Pick a realistic deep path from the middle of the dataset
        let target_path = &paths[size / 2].0;

        group.bench_with_input(BenchmarkId::new("fst", size), target_path, |b, path| {
            b.iter(|| black_box(fst.get(black_box(path))))
        });

        group.bench_with_input(BenchmarkId::new("hashmap", size), target_path, |b, path| {
            b.iter(|| black_box(hashmap.get(black_box(path))))
        });
    }

    group.finish();
}

fn bench_get_children(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_children");

    for size in [10_000, 100_000, 1_000_000, 10_000_000] {
        let paths = generate_paths(size);
        let fst_data = build_fst(&paths);
        let fst = Fst::new(&fst_data).unwrap();
        let hashmap = build_hashmap(&paths);

        // Realistic: get all children under "src/" (a common directory)
        let prefix = format!("src{}", PATH_SEP).into_bytes();

        // Count how many results we expect
        let expected_count = paths.iter().filter(|(k, _)| k.starts_with(&prefix)).count();
        println!("Size {}: 'src/' has {} children", size, expected_count);

        group.bench_with_input(BenchmarkId::new("fst_iter", size), &prefix, |b, prefix| {
            b.iter(|| {
                let results: Vec<_> = fst.prefix_iter(black_box(prefix)).collect();
                black_box(results)
            });
        });

        group.bench_with_input(BenchmarkId::new("fst_each", size), &prefix, |b, prefix| {
            b.iter(|| {
                let mut count = 0u64;
                fst.prefix_each(black_box(prefix), |_key, value| {
                    count = count.wrapping_add(value);
                    true
                });
                black_box(count)
            });
        });

        group.bench_with_input(BenchmarkId::new("hashmap", size), &prefix, |b, prefix| {
            b.iter(|| {
                let results: Vec<_> = hashmap
                    .iter()
                    .filter(|(k, _)| k.starts_with(black_box(prefix)))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                black_box(results)
            });
        });
    }

    group.finish();
}

fn bench_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_size");

    for size in [10_000, 100_000, 1_000_000, 10_000_000] {
        let paths = generate_paths(size);
        let fst_data = build_fst(&paths);
        let hashmap = build_hashmap(&paths);

        // FST: serialized size (what goes on disk / mmap)
        let fst_size = fst_data.len();

        // HashMap actual memory estimate:
        // - Each Vec<u8> key: 24 bytes (ptr+len+cap) + heap allocation (key bytes + allocator overhead)
        // - Each u64 value: 8 bytes
        // - HashMap entry overhead: ~48 bytes per entry (control bytes, padding, etc.)
        // - Bucket array: HashMap uses ~1.25x capacity for load factor
        let key_heap_bytes: usize = paths.iter().map(|(k, _)| k.len()).sum();
        let vec_overhead = 24 * hashmap.len(); // Vec<u8> struct on stack
        let value_bytes = 8 * hashmap.len();
        let entry_overhead = 48 * hashmap.len(); // HashMap internal overhead per entry
        let bucket_overhead = (hashmap.capacity() as f64 * 1.25) as usize * 8;
        let hashmap_estimated =
            key_heap_bytes + vec_overhead + value_bytes + entry_overhead + bucket_overhead;

        // Raw: just key bytes + values (theoretical minimum)
        let raw_size: usize = paths.iter().map(|(k, _)| k.len() + 8).sum();

        println!("\nSize {}:", size,);
        println!(
            "  FST (serialized):     {:>10} bytes ({:.2} MB)",
            fst_size,
            fst_size as f64 / 1_000_000.0
        );
        println!(
            "  HashMap (in-memory):  {:>10} bytes ({:.2} MB)",
            hashmap_estimated,
            hashmap_estimated as f64 / 1_000_000.0
        );
        println!(
            "  Raw data (minimum):   {:>10} bytes ({:.2} MB)",
            raw_size,
            raw_size as f64 / 1_000_000.0
        );
        println!(
            "  FST vs HashMap:       {:.2}x smaller",
            hashmap_estimated as f64 / fst_size as f64
        );
        println!(
            "  FST vs Raw:           {:.2}x larger",
            fst_size as f64 / raw_size as f64
        );

        group.bench_with_input(BenchmarkId::new("fst_bytes", size), &fst_data, |b, data| {
            b.iter(|| black_box(data.len()));
        });
    }

    group.finish();
}

/// Benchmark with high-fanout directories (many files per directory)
fn bench_high_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_fanout");

    // Test different fanout levels: 50, 100, 200, 500 files per directory
    for files_per_dir in [50, 100, 200, 500] {
        let dirs = [
            "src/components",
            "src/services",
            "src/utils",
            "lib/core",
            "lib/adapters",
        ];

        let mut generator = HighFanoutGenerator::new(42);
        let paths = generator.generate_multi_dir(files_per_dir, &dirs);
        let total_files = paths.len();

        let fst_data = build_fst(&paths);
        let fst = Fst::new(&fst_data).unwrap();

        // Pick a path from the middle for lookup benchmark
        let target_path = &paths[total_files / 2].0;

        // Print stats
        println!(
            "\nHigh-fanout {}/dir ({} total): FST = {} bytes ({:.2} MB), path len = {} bytes",
            files_per_dir,
            total_files,
            fst_data.len(),
            fst_data.len() as f64 / 1_000_000.0,
            target_path.len()
        );

        // Sample path for debugging
        if let Some((first_path, _)) = paths.first() {
            println!(
                "  Sample path: {}",
                String::from_utf8_lossy(first_path).replace('\x1f', "/")
            );
        }

        group.bench_with_input(
            BenchmarkId::new("lookup", files_per_dir),
            target_path,
            |b, path| b.iter(|| black_box(fst.get(black_box(path)))),
        );

        // Also benchmark prefix iteration on a high-fanout directory
        let prefix = format!("src{}components{}", PATH_SEP, PATH_SEP).into_bytes();
        group.bench_with_input(
            BenchmarkId::new("prefix_iter", files_per_dir),
            &prefix,
            |b, prefix| {
                b.iter(|| {
                    let count = fst.prefix_iter(black_box(prefix)).count();
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

/// Analyze node fanout distribution in the FST
fn bench_fanout_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_analysis");

    for files_per_dir in [100, 500] {
        let dirs = ["src/components"];
        let mut generator = HighFanoutGenerator::new(42);
        let paths = generator.generate_multi_dir(files_per_dir, &dirs);

        let fst_data = build_fst(&paths);

        // Just print analysis, minimal benchmark
        println!(
            "\nFanout analysis ({} files): FST size = {} bytes",
            files_per_dir,
            fst_data.len()
        );

        group.bench_with_input(
            BenchmarkId::new("size_bytes", files_per_dir),
            &fst_data,
            |b, data| b.iter(|| black_box(data.len())),
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_lookup,
    bench_get_children,
    bench_memory,
    bench_high_fanout,
    bench_fanout_analysis,
);
criterion_main!(benches);

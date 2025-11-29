# rdedupe

A fast, parallel duplicate file finder and remover written in Rust.

## Features

- Parallel file scanning using [rayon](https://github.com/rayon-rs/rayon)
- Progress bars with [indicatif](https://github.com/console-rs/indicatif)
- DataFrame analysis with [Polars](https://github.com/pola-rs/polars)
- Optional CSV report generation
- Safe deletion with dry-run preview

## Installation

```bash
# Build from source
make release
make install-user

# Or install directly
cargo install --path .
```

## Usage

```bash
# Report duplicates in current directory
rdedupe .

# Report duplicates matching a pattern
rdedupe /path/to/dir --pattern .txt

# Generate CSV report
rdedupe . --pattern .jpg --csv report.csv

# Preview what would be deleted
rdedupe . --delete --dry-run

# Delete duplicates (keeps first file alphabetically in each group)
rdedupe . --delete --force
```

## Building

```bash
make              # Show all available targets
make build        # Debug build
make release      # Optimized release build
make static       # Static Linux binary (musl)
make test         # Run tests
make lint         # Run clippy
```

## How It Works

1. Recursively walks the directory tree
2. Filters files by pattern (if specified)
3. Computes MD5 hash for each file in parallel
4. Creates a Polars DataFrame with file metadata
5. Uses Polars window functions to identify duplicates (files sharing the same hash)
6. Reports or deletes duplicates (keeping first file alphabetically per group)

## License

This project is derived from [noahgift/rdedupe](https://github.com/noahgift/rdedupe) by Noah Gift.

See [LICENSE](LICENSE) for details.

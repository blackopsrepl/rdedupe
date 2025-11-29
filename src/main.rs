use clap::Parser;

#[derive(Parser)]
#[clap(
    version = "2.0",
    author = "Vittorio Distefano",
    about = "Finds and removes duplicate files",
    after_help = "Examples:\n  rdedupe .                     # Report duplicates in current dir\n  rdedupe /path --pattern .txt  # Report .txt duplicates\n  rdedupe . --delete --dry-run  # Preview what would be deleted\n  rdedupe . --delete --force    # Delete duplicates without confirmation"
)]
struct Cli {
    // Directory to scan for duplicates
    #[clap(default_value = ".")]
    path: String,

    // File extension pattern to match (e.g., .txt, .jpg)
    #[clap(long, default_value = "")]
    pattern: String,

    // Generate detailed CSV report
    #[clap(long)]
    csv: Option<String>,

    // Delete duplicate files (keeps first file in each group)
    // TODO: keep newest
    #[clap(long)]
    delete: bool,

    // Preview deletions without actually deleting
    #[clap(long, requires = "delete")]
    dry_run: bool,

    // Skip confirmation prompt when deleting
    #[clap(long, requires = "delete")]
    force: bool,
}

fn main() {
    let cli = Cli::parse();

    rdedupe::display_thread_info();
    println!("Analyzing files in {} matching '{}'", cli.path, cli.pattern);

    let result = rdedupe::run_with_dataframe(&cli.path, &cli.pattern, cli.csv.as_deref());

    match result {
        Ok(df) => {
            println!("\n=== Analysis Complete ===");
            println!("Total files analyzed: {}", df.height());

            if let Some(ref csv_path) = cli.csv {
                println!("Detailed CSV report saved to: {}", csv_path);
            }

            if cli.delete {
                println!("\n=== Delete Mode ===");
                match rdedupe::delete_duplicates(&df, cli.dry_run, cli.force) {
                    Ok(result) => {
                        if result.dry_run {
                            println!("\n(Dry run complete - no files were deleted)");
                        } else if result.deleted_count > 0 {
                            println!("\nDeletion complete!");
                        }
                    }
                    Err(e) => {
                        eprintln!("Error during deletion: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

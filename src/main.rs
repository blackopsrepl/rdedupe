use clap::Parser;
use rdedupe::cli::Cli;
use rdedupe::commands::{
    dispatch, EXIT_BROKEN_PIPE, EXIT_CANCELLED, EXIT_FAILED, EXIT_OK, EXIT_USAGE,
};
use rdedupe::scheduler::Cancellation;

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            // --help and --version are successful requests printed to stdout; anything else
            // is an invalid invocation with its own exit code, distinct from a partial run.
            let code = if error.use_stderr() {
                EXIT_USAGE
            } else {
                EXIT_OK
            };
            let _ = error.print();
            std::process::exit(code);
        }
    };
    let cancellation = Cancellation::default();
    let handler = cancellation.clone();
    if let Err(error) = ctrlc::set_handler(move || handler.cancel()) {
        eprintln!("warning: cannot install the interrupt handler: {error}");
    }
    let code = match dispatch(cli, &cancellation) {
        Ok(code) => code,
        Err(rdedupe::Error::Cancelled) => EXIT_CANCELLED,
        Err(error) if error.is_broken_pipe() => EXIT_BROKEN_PIPE,
        Err(error) => {
            eprintln!(
                "error: {}",
                rdedupe::paths::display_text(&error.to_string())
            );
            let category = serde_json::to_value(error.category())
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            if error.retry_safe() {
                eprintln!("error-category: {category}; retry-safe: yes (nothing was changed)");
            } else {
                eprintln!(
                    "error-category: {category}; retry-safe: no (files may have changed: run `rdedupe recover --journal` before retrying)"
                );
            }
            EXIT_FAILED
        }
    };
    std::process::exit(code);
}

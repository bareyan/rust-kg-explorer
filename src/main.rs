mod store;
mod utils;
mod item;

mod web_ui;

use dotenv::dotenv;
use clap::Parser;
use web_ui::server::WebServer;

use crate::{ store::KG };

/// For parsing command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Enable WDC(Web Data Commons) mode
    #[arg(short = 'w', long = "wdc")]
    wdc: bool,

    /// Dataset name or rdf file path (required)
    #[arg(long)]
    dataset: String,

    /// Number of parts (default = 1)
    #[arg(long, default_value_t = 1)]
    nb_parts: u32,
}

fn main() {
    dotenv().ok();
    let args = Args::parse();
    if args.wdc {
        // If wdc flag is there, download and load from web data commons

        let kg = KG::from_wdc(&args.dataset, args.nb_parts);
        let w = WebServer::new(kg, 8080);
        w.serve();
    } else {
        // Otherwise load from the filepath specified as the dataset
        let kg = KG::from_file(&args.dataset);
        let w = WebServer::new(kg, 8080);
        w.serve();
    }
}

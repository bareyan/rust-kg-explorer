
mod store;
mod utils;
mod item;
mod web_ui;

use clap::Parser;
use web_ui::server::WebServer;

use crate::{store::KG};



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
   let args = Args::parse();
    if args.wdc {
        let kg = KG::new(&args.dataset, args.nb_parts);
        let w = WebServer::new(kg ,8080);
        w.serve();
    }
    else{
        let kg = KG::from_file(&args.dataset);
        let w = WebServer::new(kg ,8080);
        w.serve();
        
    }

}
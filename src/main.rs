
mod store;
mod utils;
mod item;
mod web_ui;

use std::env;
use web_ui::server::WebServer;


fn main() {
    let args: Vec<String> = env::args().collect();
    let w = WebServer::new(&args[1], 8080);
    w.serve();
}



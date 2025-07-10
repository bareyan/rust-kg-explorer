# Rust KG Explorer

A simple web-based knowledge graph explorer built with Rust.

## Overview

Rust KG Explorer provides a lightweight web UI to explore and query a knowledge graph. It supports browsing entities and executing SPARQL queries.

## Requirements

- Rust (latest stable)
- Cargo
- A valid dataset name (for Web Data Commons mode) or a local RDF file (for file mode)

**Datasets:** The datasets are pulled from Web Data Commons Class Specific Datasets.

## How to Use

1. Clone the repository.
2. Build the project:
   ```
   cargo build --release
   ```
3. Run the project using one of the following methods:

### Running in Web Data Commons Mode

Pass the `--wdc` flag along with the dataset name and (optionally) the number of parts.  
Example:

```
cargo run -- --wdc --dataset Book --nb-parts 3
```

### Running in File Mode

If you want to load an existing RDF file, omit the `--wdc` flag and provide the file path as the dataset.
Example:

```
cargo run -- --dataset /path/to/my_file.nt
```

4. Open your browser and navigate to: [http://127.0.0.1:8080](http://127.0.0.1:8080)

## Project Structure

- `src/`: Contains all Rust source files.
- `data/`: Stores the downloaded and preprocessed KG data.
- `templates/`: Contains all of the templates for the web pages
- `routines/`: Contains all the routine files

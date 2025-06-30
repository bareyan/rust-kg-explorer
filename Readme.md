# Rust KG Explorer

A simple web-based knowledge graph explorer built with Rust.

## Overview

Rust KG Explorer provides a lightweight web UI to explore and query a knowledge graph. It supports browsing entities and executing SPARQL queries.
Common Crawl corpus and created multiple schema.org class-specific subsets.

## Requirements

- Rust (latest stable)
- Cargo
- A valid dataset (pass the dataset name as an argument)

**Datasets:** The datasets are pulled from Web Data Commons Class-Specific Datasets. The dataset will download and extract automatically, just with the name of the class.

## How to Use

1. Clone the repository.
2. Build the project:
   ```
   cargo build --release
   ```
3. Run the project with the dataset name as the first argument:
   ```
   cargo run <DATASET_NAME>
   ```
   Example:
   ```
   cargo run my_dataset
   ```
4. Open your browser and navigate to: [http://127.0.0.1:8080](http://127.0.0.1:8080)

## Project Structure

- `src/`: Contains all Rust source files.
- `data/`: Stores the downloaded and preprocessed KG data.

## License

This project is licensed under the MIT License.

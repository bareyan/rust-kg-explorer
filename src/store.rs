//! # Knowledge Graph Store Handler
//!
//! This file defines the `KG` struct and its associated methods for managing a knowledge graph dataset.
//! It provides functionality for downloading, loading, querying, updating, and managing RDF datasets using the Oxigraph library.
//!
//! ## Key Features
//! - **Dataset Management**: Load datasets from WDC or local files, preprocess RDF data, and store it in an Oxigraph store.
//! - **SPARQL Querying**: Execute SPARQL `SELECT`, `UPDATE`, and iterative queries on the knowledge graph.
//! - **Version Control**: Dump and revert the store to specific versions, maintaining a history of operations.
//! - **Entity Management**: Merge entities based on shared predicates, retrieve entity details, and fetch associated images.
//! - **History Replay**: Replay operations from a history file or routine files.
//!
//! ## Structs and Enums
//! - `KG`: Represents the knowledge graph store and provides methods for dataset handling and SPARQL operations.
//! - `StoreError`: Enumerates possible errors during store operations, such as evaluation errors or unsupported query types.

use core::option::Option::None;
use core::panic;
use core::result::Result;

use std::collections::{ HashMap };
//Working with files
use std::path::Path;
use std::fs::{ read_to_string, File };
use std::io::{ Write };

use std::str::FromStr;

// Timing procedures
use std::time::Instant;

// Mulithread handling
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::sync::Arc;

// Tar-gz decoder
use flate2::read::GzDecoder;

// Oxigraph imports
use oxigraph::model::{ GraphNameRef, NamedNode, Term };
use oxigraph::model::Term::Literal;
use oxigraph::store::Store;
use oxigraph::sparql::{ QueryResults, QuerySolution };
use oxigraph::io::{ RdfParser, RdfFormat };

// Petgraph

use petgraph::graph::EdgeIndex;
use petgraph::graph::NodeIndex;
use petgraph::visit::{ EdgeRef };
use petgraph::Direction::{ Incoming, Outgoing };
use petgraph::{ self, data, Graph };
use rayon::iter::{ IntoParallelRefIterator, ParallelIterator };
use rayon::result;
// Create imports
use crate::utils::{
    self,
    calculate_probabilities_for_graph,
    choice,
    compute_scores,
    extract_literal,
    load_predicate_analysis,
    load_relations,
    normalize_column,
    remove_disconnected,
    save_predicate_anlaysis,
    save_relations,
};
use crate::item;

/// # Enumerates possible errors during store operations.
///
/// ## Variants:
/// * `EvaluationError(String)`: Error thrown when SPARQL evaluation fails.
/// * `UnsupportedError`: Indicates that the requested operation or query result type is not supported.
pub enum StoreError {
    EvaluationError(String),
    UnsupportedError,
}

/// # Configuration and storage handler for a knowledge graph dataset.
/// ## Fields
/// * `dataset` - Name of the WDC dataset or path to a local dataset file.
/// * `nb_parts` - Number of parts to download when fetching a WDC dataset.
/// * `history_path` - File path where download history is recorded.
/// * `store` - Store for managing and persisting the dataset.
pub struct KG {
    dataset: String,
    nb_parts: u32,
    history_path: String,
    store: Option<Store>,
}

impl KG {
    /// # Constructors

    /// Constructs a `KG` by downloading (if needed) and loading a WDC dataset.
    ///
    /// - Downloads and unpacks dataset parts if the local SQLite store does not exist.
    /// - Loads data into an Oxigraph store.
    ///
    /// Parameters:
    /// - `dataset_name`: Identifier of the WDC dataset.
    /// - `nb_parts`: Number of parts to fetch and process.
    pub fn from_wdc(dataset_name: &str, nb_parts: u32) -> KG {
        let mut created = KG {
            dataset: dataset_name.to_string(),
            nb_parts,
            store: None,
            history_path: String::new(),
        };

        //Check if the store is not yet created and download the dataset if needed
        if !Path::new(&format!("./data/{}.db", dataset_name.to_lowercase())).exists() {
            created.download_dataset();
        }
        created.load_wdc();

        created
    }

    /// Constructs a `KG` by loading a dataset from a local file.
    ///
    /// - Detects RDF format from file extension (`.ttl`, `.nt`, `.nq`, or `.db`).
    /// - Loads or initializes an Oxigraph store.
    ///
    /// Parameter:
    /// - `dataset_path`: Path to the local dataset file.
    pub fn from_file(dataset_path: &str) -> KG {
        let mut created = KG {
            dataset: dataset_path.to_string(),
            nb_parts: 0,
            store: None,
            history_path: String::new(),
        };
        created.load_file(dataset_path);

        created
    }

    // # Loading procedures

    /// Downloads, unpacks, and preprocesses parts of a WDC dataset.
    ///
    /// - Creates the data directory for the dataset.
    /// - Downloads `.gz` part files from the WDC server.
    /// - Unzips each part file and preprocesses the resulting N-Triples.
    /// - Cleans up intermediate files upon successful processing.
    fn download_dataset(&self) {
        let mut now = Instant::now();

        let download_path = format!(
            "https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/{}/part_",
            self.dataset
        );

        //Path to the directory where the dataset rdfs will be stored
        let path = format!("./data/{}", self.dataset);

        //Creating the directory if it does not exist
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path).expect("Failed to create directory");
        }

        // Check that all of the parts are downloaded, download them if not
        for i in 0..self.nb_parts {
            let part_path = format!("{}/part_{}.gz", path, i);

            //Check if the part file is already downloaded, or unpacked
            if
                Path::new(&part_path).exists() ||
                Path::new(&part_path.replace(".gz", ".nt")).exists() ||
                Path::new(&part_path.replace(".gz", "")).exists()
            {
                println!("Part {} loaded.", i);
            } else {
                let url = format!("{}{}.gz", download_path, i);
                let client = reqwest::blocking::Client
                    ::builder()
                    .timeout(std::time::Duration::from_secs(300)) // 5 minutes timeout
                    .build()
                    .expect("Failed to build HTTP client");
                let response = client.get(&url).send().expect("Failed to download part");
                let mut f = std::fs::OpenOptions
                    ::new()
                    .create(true)
                    .write(true)
                    .open(&part_path)
                    .unwrap();
                if let Ok(bytes) = response.bytes() {
                    let _ = f.write_all(&bytes);
                    println!("Downloaded part {} to {}", i, part_path);
                } else {
                    panic!("Failed to load bytes from the response!");
                }
            }
        }

        println!("Downloaded part files in {:.2?}", now.elapsed());
        now = Instant::now();
        //Unzip the part files
        for i in 0..self.nb_parts {
            //unzip the part file
            let part_path = format!("./data/{}/part_{}.gz", self.dataset, i);
            let output_path = format!("./data/{}/part_{}", self.dataset, i);

            if
                Path::new(&output_path).exists() ||
                Path::new(&part_path.replace(".gz", ".nt")).exists()
            {
                println!("Part {} already unzipped, skipping.", i);
            } else {
                let mut decoder = GzDecoder::new(
                    File::open(&part_path).expect("Failed to open part file")
                );
                let mut output = File::create(&output_path).expect("Failed to create output file");
                std::io::copy(&mut decoder, &mut output).expect("Failed to unzip part file");
                println!("Unzipped part {} to {}", i, output_path);
            }
        }
        println!("Unzipped part files in {:.2?}", now.elapsed());
        now = Instant::now();

        for i in 0..self.nb_parts {
            let part_path = format!("./data/{}/part_{}.gz", self.dataset, i);
            let output_path = format!("./data/{}/part_{}", self.dataset, i);
            if
                Path::new(&output_path).exists() &&
                !Path::new(&part_path.replace(".gz", ".nt")).exists()
            {
                utils::preprocess(&output_path);

                //delete the gz and and unzipped file
                std::fs::remove_file(&part_path).expect("Failed to delete part file");
                std::fs::remove_file(&output_path).expect("Failed to delete unzipped file");
            }
        }
        println!("Preprocessed part files in {:.2?}", now.elapsed());
    }

    /// Loads a WDC dataset into the Oxigraph store.
    ///
    /// - Opens or creates the SQLite-backed Oxigraph store.
    /// - Bulk-loads all N-Triples parts if the store is empty (parallelized).
    /// - Tracks and reports parse errors.
    /// - Initializes `history_path` for operation logging.
    fn load_wdc(&mut self) {
        let now = Instant::now();
        // Load the oxigraph database
        let store = Store::open(format!("./data/{}.db", self.dataset.to_lowercase())).expect(
            "Failed to load database"
        );
        let is_empty = store.is_empty().expect("Failed to check if store is empty");
        if is_empty {
            let ignored_lines_count = Arc::new(AtomicUsize::new(0));
            // Load the graph from the nt files
            for i in 0..self.nb_parts {
                let part_path = format!("./data/{}/part_{}.nt", self.dataset, i);
                let reader = File::open(&part_path).expect("Failed to open part file");
                let parser = RdfParser::from_format(RdfFormat::NTriples);
                let count_clone = Arc::clone(&ignored_lines_count);

                store
                    .bulk_loader()
                    .with_num_threads(16)
                    .on_parse_error(move |_err| {
                        count_clone.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    })
                    .load_from_reader(parser, reader)
                    .expect("Failed to load NTriples");
            }
            let final_count = ignored_lines_count.load(Ordering::Relaxed);
            println!(
                "Data loading complete in {:.2?}. Total ignored lines: {}",
                now.elapsed(),
                final_count
            );
        } else {
            println!("Graph loaded");
        }
        self.store = Some(store);

        self.history_path = format!("./data/{}.db/history.txt", self.dataset.to_lowercase());
        // Set up history file
    }

    /// Loads a local RDF or Oxigraph database file into the store.
    ///
    /// - Determines the RDF format or SQLite DB based on file extension.
    /// - Bulk-loads data if the created store is empty.
    /// - Initializes `history_path` for operation logging.
    fn load_file(&mut self, file_path: &str) {
        let filename = match file_path.split("/").last() {
            Some(f) => f,
            _ => panic!("Invalid file path"),
        };
        let file_format = match filename.split(".").last() {
            Some(f) =>
                match f {
                    "ttl" => RdfFormat::Turtle,
                    "nt" => RdfFormat::NTriples,
                    "nq" => RdfFormat::NQuads,
                    "db" => {
                        self.store = Some(Store::open(file_path).expect("Failed to load from db"));
                        return;
                    }
                    _ => panic!("Format not supported"),
                }
            None => panic!("Provide a file with the following extentions: .ttl, .nt, .nq"),
        };
        let store = Store::open(format!("./data/{}.db", filename)).expect(
            "Failed to load database"
        );
        let is_empty = store.is_empty().expect("Failed to check if store is empty");
        if is_empty {
            let ignored_lines_count = Arc::new(AtomicUsize::new(0));
            let reader = File::open(file_path).expect("Failed to open part file");
            let parser = RdfParser::from_format(file_format);
            let count_clone = Arc::clone(&ignored_lines_count);

            store
                .bulk_loader()
                .with_num_threads(16)
                .on_parse_error(move |_err| {
                    count_clone.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
                .load_from_reader(parser, reader)
                .expect("Failed to load file");

            let final_count = ignored_lines_count.load(Ordering::Relaxed);
            println!("Data loading complete. Total ignored lines: {}", final_count);
        } else {
            println!("Graph loaded");
        }
        self.store = Some(store);

        // Set up the history file
        self.history_path = format!("./data/{}.db/history.txt", filename);
    }

    // # Getters

    /// Returns the base name of the loaded dataset.
    ///
    /// - For WDC datasets, returns the `dataset` field.
    /// - For file-based datasets, strips known extensions (`.nt`, `.ttl`, `.db`, `.nq`).
    pub fn get_name(&self) -> String {
        if self.nb_parts > 0 {
            return self.dataset.clone();
        } else {
            self.dataset
                .split("/")
                .last()
                .unwrap_or(&self.dataset)
                .replace(".nt", "")
                .replace(".ttl", "")
                .replace(".db", "")
                .replace(".nq", "")
        }
    }

    // # History

    /// Appends an operation to the history file.
    ///
    /// - Ensures the history file exists.
    /// - Writes the provided content as a new line.
    pub fn write_to_history(&self, content: String) {
        if
            let Ok(mut file) = std::fs::OpenOptions
                ::new()
                .create(true)
                .append(true)
                .open(self.history_path.clone())
        {
            let _ = writeln!(file, "{}", content);
        }
    }

    /// Reads and returns the entire content of the history file.
    ///
    /// # Panics
    /// Panics if the history file cannot be read.
    pub fn get_history(&self) -> String {
        read_to_string(self.history_path.clone()).unwrap()
    }

    // # Store operations

    /// Executes a SPARQL `SELECT`('CONSTRUCT', `ASK`, or `DESCRIBE` to be implemented) query against the store.
    ///
    /// Returns a vector of `QuerySolution` on success.
    ///
    /// # Errors
    /// - `StoreError::EvaluationError` if the query fails to evaluate.
    /// - `StoreError::UnsupportedError` if the query result type is not supported.
    pub fn query(&self, query: &str) -> Result<Vec<QuerySolution>, StoreError> {
        if let Some(store) = &self.store {
            let result = store.query(query);
            match result {
                Ok(QueryResults::Solutions(query_solution_iter)) => {
                    let mut result: Vec<QuerySolution> = vec![];
                    for sol in query_solution_iter {
                        match sol {
                            Ok(solution) => {
                                result.push(solution);
                            }
                            Err(_) => panic!("Some error accured with the request"),
                        }
                    }
                    Ok(result)
                }
                Ok(_) => Err(StoreError::UnsupportedError),
                Err(e) => Err(StoreError::EvaluationError(e.to_string())),
            }
        } else {
            panic!("Store is not initialized");
        }
    }

    /// Executes a SPARQL update (`INSERT`/`DELETE`) query against the store.
    ///
    /// # Errors
    /// Returns `StoreError::EvaluationError` if the update fails.
    pub fn update(&self, query: &str) -> Result<(), StoreError> {
        if let Some(store) = &self.store {
            let r = store.update(query);
            match r {
                Ok(_) => Ok(()),
                Err(e) => Err(StoreError::EvaluationError(e.to_string())),
            }
        } else {
            panic!("Store is not initialized");
        }
    }

    /// Runs an iterative SPARQL update based on a `SELECT` query and an update template.
    ///
    /// - Executes `select_query` to retrieve bindings.
    /// - For each result row, replaces `{{variable}}` placeholders in `update_query`.
    /// - Executes the generated update for each row.
    ///
    /// # Errors
    /// - `StoreError::EvaluationError` if either the select or update queries are invalid.
    pub fn iterative_update(
        &self,
        select_query: &str,
        update_query: &str
    ) -> Result<(), StoreError> {
        let select_result = self.query(select_query);
        match select_result {
            Ok(result) => {
                if result.is_empty() {
                    return Ok(());
                }
                let vars = result
                    .get(0)
                    .unwrap()
                    .variables()
                    .iter()
                    .map(|v| v.as_str())
                    .collect::<Vec<&str>>();

                for r in &result {
                    let mut uq = update_query.to_string();
                    for v in &vars {
                        let var = r.get(*v).unwrap().to_string();
                        uq = uq.replace(&format!(r#"{{{{{v}}}}}"#), &var);
                    }
                    match self.update(&uq) {
                        Ok(_) => (),
                        Err(_) => {
                            return Err(
                                StoreError::EvaluationError("Invalid update query".to_string())
                            );
                        }
                    }
                }
                println!("Ran {} queries", result.len());

                Ok(())
            }
            Err(_) => { Err(StoreError::EvaluationError("Invalid Select Query".to_string())) }
        }
    }

    // # Version management

    /// Dumps the current graph state to a new N-Triples file.
    ///
    /// - Creates a `data/<dataset>/` directory if missing.
    /// - Names the dump file `version_<N>.nt`, where `N` is the next available version number.
    /// - Appends a dump record to the history file.
    /// - Serializes the default graph to N-Triples.
    pub fn dump_store(&self) {
        if let Some(store) = &self.store {
            let dir_path = format!(
                "./data/{}/",
                self.dataset
                    .split("/")
                    .last()
                    .unwrap_or(&self.dataset)
                    .replace(".nt", "")
                    .replace(".ttl", "")
                    .replace(".db", "")
                    .replace(".nq", "")
            );

            if !Path::new(&dir_path).is_dir() {
                std::fs::create_dir_all(&dir_path).expect("Failed to create directory");
            }

            let mut version = 1;
            loop {
                let file_path = format!("{}version_{}.nt", dir_path, version);
                if !Path::new(&file_path).exists() {
                    break;
                }
                version += 1;
            }

            let file_path = format!("{}version_{}.nt", dir_path, version);

            println!("Dumping store to {}", file_path);

            if
                let Ok(mut file) = std::fs::OpenOptions
                    ::new()
                    .create(true)
                    .append(true)
                    .open(
                        format!(
                            "./data/{}.db/history.txt",
                            self.dataset.to_lowercase().split("/").last().unwrap_or(&self.dataset)
                        )
                    )
            {
                let _ = writeln!(file, "Dumping store to {}", file_path);
            }

            let mut file = File::create(&file_path).expect("Failed to create dump file");

            let mut buffer = Vec::new();
            let _ = store.dump_graph_to_writer(
                GraphNameRef::DefaultGraph,
                RdfFormat::NTriples,
                &mut buffer
            );

            let _ = file.write(&buffer);
        }
    }

    /// Reverts the store to a previous dumped version.
    ///
    /// - Clears the current store.
    /// - Loads `version_<version>.nt` from the dataset directory.
    /// - Truncates the history file to the revert point.
    /// - Removes any newer dump files.
    pub fn revert(&self, version: u32) {
        if let Some(store) = &self.store {
            let dataset = self.dataset.split("/").last().unwrap_or(&self.dataset);
            let _ = store.clear();
            let dir_path = format!(
                "./data/{}/",
                dataset.replace(".nt", "").replace(".ttl", "").replace(".db", "").replace(".nq", "")
            );

            let file_path = format!("{}version_{}.nt", dir_path, version);
            let parser = File::open(file_path).unwrap();
            store
                .bulk_loader()
                .with_num_threads(16)
                .load_from_reader(RdfParser::from_format(RdfFormat::NTriples), parser)
                .expect("Failed to load file");

            let history_path = format!("./data/{}.db/history.txt", dataset.to_lowercase());
            if let Ok(content) = std::fs::read_to_string(&history_path) {
                let target_line = format!(
                    "Dumping store to ./data/{}/version_{}.nt",
                    dataset
                        .replace(".nt", "")
                        .replace(".ttl", "")
                        .replace(".db", "")
                        .replace(".nq", ""),
                    version
                );
                if let Some(pos) = content.find(&target_line) {
                    let end_pos = pos + target_line.len();
                    if let Some(newline_pos) = content[end_pos..].find('\n') {
                        let truncated_content = &content[..end_pos + newline_pos + 1];
                        let _ = std::fs::write(&history_path, truncated_content);
                    }
                }
            }
            let mut v = version + 1;
            loop {
                let file_path = format!("{}version_{}.nt", dir_path, v);
                if !Path::new(&file_path).exists() {
                    break;
                }
                let _ = std::fs::remove_file(&file_path);
                v += 1;
            }
        }
    }

    /// Replays a history of operations from a multi-line string.
    ///
    /// - Parses SPARQL blocks delimited by ```sparql ... ``` and executes them.
    /// - Supports advanced queries with a `#\n` separator for `SELECT` + `UPDATE`.
    /// - Executes routine files referenced as `file::procedure` lines.
    /// - Logs each replayed line back to the history file.
    pub fn execute(&self, content: String) -> Result<(), (StoreError, i32)> {
        let lines = content.lines().map(str::trim);
        let mut in_sparql = false;
        let mut sparql_block = String::new();
        let mut count = 0;
        for line in lines {
            if line.starts_with("```sparql") {
                in_sparql = true;
                sparql_block.clear();
            } else if line.starts_with("```") && in_sparql {
                in_sparql = false;

                // Execute the SPARQL block
                if sparql_block.contains("#\n") {
                    // Advanced Query Detected
                    let parts: Vec<&str> = sparql_block.split("#\n").collect();
                    if parts.len() == 2 {
                        let (select_query, update_query) = (parts[0].trim(), parts[1].trim());
                        match self.iterative_update(select_query, update_query) {
                            Ok(_) => {
                                count += 1;
                            }
                            Err(e) => {
                                return Err((e, count));
                            }
                        };
                    } else {
                        // Regular Update Query
                        match self.update(&sparql_block) {
                            Ok(_) => {
                                count += 1;
                            }
                            Err(e) => {
                                return Err((e, count));
                            }
                        };
                    }
                } else {
                    // Regular SPARQL update
                    match self.update(&sparql_block) {
                        Ok(_) => {
                            count += 1;
                        }
                        Err(e) => {
                            return Err((e, count));
                        }
                    };
                }
            } else if in_sparql {
                sparql_block.push_str(line);
                sparql_block.push('\n');
            } else if line.contains("::") && !line.starts_with("Dumping") {
                // Executing a routine
                let (file, proc) = line.split_once("::").unwrap();
                let path = Path::new("routines").join(file);

                if let Ok(routine_content) = read_to_string(&path) {
                    let mut current_name = String::new();
                    let mut current_query = String::new();
                    let mut in_proc = false;
                    let mut is_advanced = false;

                    for routine_line in routine_content.lines() {
                        if routine_line.starts_with("##") {
                            if in_proc && current_name == proc {
                                // Execute the found procedure
                                if is_advanced {
                                    let parts: Vec<&str> = current_query.split("#\n").collect();
                                    if parts.len() == 2 {
                                        let (select_query, update_query) = (
                                            parts[0].trim(),
                                            parts[1].trim(),
                                        );
                                        match self.iterative_update(select_query, update_query) {
                                            Ok(_) => {
                                                count += 1;
                                            }
                                            Err(e) => {
                                                return Err((e, count));
                                            }
                                        }
                                    } else {
                                        match self.update(&current_query) {
                                            Ok(_) => {
                                                count += 1;
                                            }
                                            Err(e) => {
                                                return Err((e, count));
                                            }
                                        };
                                    }
                                } else {
                                    match self.update(&current_query) {
                                        Ok(_) => {
                                            count += 1;
                                        }
                                        Err(e) => {
                                            return Err((e, count));
                                        }
                                    };
                                }
                                break;
                            }
                            is_advanced = routine_line.ends_with("@advanced");
                            current_name = routine_line.trim_start_matches("##").trim().to_string();
                            current_query.clear();
                            in_proc = true;
                        } else if in_proc {
                            current_query.push_str(routine_line);
                            current_query.push('\n');
                        }
                    }

                    // Handle case last procedure
                    if in_proc && current_name == proc {
                        if is_advanced {
                            let parts: Vec<&str> = current_query.split("#\n").collect();
                            if parts.len() == 2 {
                                let (select_query, update_query) = (
                                    parts[0].trim(),
                                    parts[1].trim(),
                                );
                                match self.iterative_update(select_query, update_query) {
                                    Ok(_) => {
                                        count += 1;
                                    }
                                    Err(e) => {
                                        return Err((e, count));
                                    }
                                };
                            } else {
                                match self.update(&current_query) {
                                    Ok(_) => {
                                        count += 1;
                                    }
                                    Err(e) => {
                                        return Err((e, count));
                                    }
                                };
                            }
                        } else {
                            match self.update(&current_query) {
                                Ok(_) => {
                                    count += 1;
                                }
                                Err(e) => {
                                    return Err((e, count));
                                }
                            };
                        }
                    }
                }
            }
            if !line.starts_with("Dumping") {
                self.write_to_history(format!("{}", line));
            }
        }

        Ok(())
    }

    // # Useful procedures

    /// Counts the number of triples in the default graph.
    ///
    /// Executes:
    /// ```sparql
    /// SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }
    /// ```
    ///
    /// Returns the parsed count or 0 on error.
    pub fn count_lines(&self) -> u64 {
        let query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }";
        match self.query(query) {
            Ok(solutions) => {
                if let Some(solution) = solutions.first() {
                    if let Some(count_term) = solution.get("count") {
                        if let Some(count_str) = extract_literal(Some(count_term)) {
                            if let Ok(count) = count_str.parse::<u64>() {
                                return count;
                            }
                        }
                    }
                }
                0
            }
            Err(_) => 0,
        }
    }

    /// Merges entities of the same type that share all specified predicate-object pairs.
    ///
    /// - Constructs a SPARQL `SELECT` to find pairs of subjects (`?s1`, `?s2`) of type `ent`.
    /// - Uses `merge_using` predicates to ensure matching objects.
    /// - For each pair, deletes references to `?s2` and replaces them with `?s1`, then removes `?s2` triples.
    /// - Records the SPARQL in history.
    pub fn merge_entities(&self, ent: String, merge_using: Vec<String>) -> Result<(), StoreError> {
        let mut criteres = String::new();
        // Create lines in the select query corresponding to matches for each of the merge_using predicates
        for (i, m) in merge_using.iter().enumerate() {
            criteres += &format!("?s1 {m} ?o{i}. ?s2 {m} ?o{i}.");
        }

        //Construct the select query
        let q = format!(
            r#"SELECT ?s1 ?s2 WHERE  {{
    ?s1 a {0}.
    ?s2 a {0}.
    {criteres}
    FILTER(STR(?s1) < STR(?s2))
}}
        "#,
            ent
        );

        //Execute an iterative update
        let r = self.iterative_update(
            &q,
            r#"DELETE { ?sub ?pred {{s2}} }
INSERT { ?sub ?pred {{s1}} }
WHERE  { ?sub ?pred {{s2}} };
DELETE { {{s2}} ?p ?o }
INSERT { {{s1}} ?p ?o }
WHERE  { {{s2}} ?p ?o }
        "#
        );

        self.write_to_history(
            format!(
                "```sparql\n{}\n#\n{}```",
                q,
                r#"
DELETE { ?sub ?pred {{s2}} }
INSERT { ?sub ?pred {{s1}} }
WHERE  { ?sub ?pred {{s2}} };
DELETE { {{s2}} ?p ?o }
INSERT { {{s1}} ?p ?o }
WHERE  { {{s2}} ?p ?o }
        "#
            )
        );
        r
    }

    /// Retrieves a page of entity IRIs of a given type.
    ///
    /// - `object_type`: IRI of the RDF type to filter on.
    /// - `limit`: Maximum number of results.
    /// - `offset`: Number of items to skip.
    ///
    /// Returns a vector of `Term::NamedNode` matching the type.
    pub fn get_objects(&self, object_type: &str, limit: u32, offset: u32) -> Vec<Term> {
        let q = format!(
            "
            SELECT DISTINCT ?obj WHERE {{
                ?obj a {}.
            }}
            LIMIT {}
            OFFSET {}
        ",
            object_type,
            limit,
            offset
        );
        let result = self.query(&q).unwrap_or(vec![]);
        let mut res = vec![];

        for sol in result {
            res.push(sol.get("obj").unwrap().clone());
        }
        res
    }

    /// Fetches detailed information for an entity given as a stringet_countsg IRI.
    ///
    /// - Gathers all RDF types, the first `schema:name`, and the first `schema:description`.
    /// - Determines if the entity is an image type.
    /// - Collects images via `get_images`.
    ///
    /// Returns an `item::Item`.
    pub fn get_details(&self, object: &str) -> item::Item {
        let mut otypes: Vec<Term> = vec![];
        let type_query =
            format!("
        SELECT ?otype WHERE {{
            {} a ?otype .
        }}
    ", object);
        let name_query =
            format!("
            SELECT ?name WHERE {{
                {} <http://schema.org/name> ?name .
            }}
            LIMIT 1
        ", object);
        let description_query =
            format!("
            SELECT ?description WHERE {{
                {} <http://schema.org/description> ?description .
            
            }}
            LIMIT 1
        ", object);
        let typer = self.query(&type_query).unwrap_or(vec![]);

        let namer = self.query(&name_query).unwrap_or(vec![]);
        let descriptionr = self.query(&description_query).unwrap_or(vec![]);

        for tp in typer {
            otypes.push(tp.get("otype").unwrap().clone());
        }
        let is_img = otypes.contains(
            &NamedNode::from_str("<http://schema.org/ImageObject>").unwrap().into()
        );
        // let otype = if typer.is_empty() {None} else {typer.iter().next().unwrap().get("otype")};
        let name = if namer.is_empty() {
            None
        } else {
            extract_literal(namer.first().unwrap().get("name"))
        };
        let description = if descriptionr.is_empty() {
            None
        } else {
            extract_literal(descriptionr.first().unwrap().get("description"))
        };

        let node = NamedNode::from_str(object).unwrap_or_else(|_|
            panic!("Failed to create object from string! {object}")
        );
        item::Item::new(node.into(), otypes, name, description, self.get_images(object, is_img))
    }

    /// Retrieves image URLs or paths associated with a subject.
    ///
    /// - If `is_img` is true, queries `schema:url`.
    /// - Otherwise, queries common predicates (`schema:image`, `schema:photo`, `schema:logo`, `foaf:depiction`).
    /// - Validates each URL/path before inclusion.
    fn get_images(&self, object: &str, is_img: bool) -> Vec<String> {
        let query_image = if is_img {
            format!(
                r#"
            SELECT ?img WHERE {{
        {object} <http://schema.org/url> ?img .
            }}
                
                "#
            )
        } else {
            format!(
                r#"
        SELECT ?img WHERE {{
          {{
            {0} <http://schema.org/image> ?img .
          }}
          UNION {{
            {0} <http://schema.org/photo> ?img .
          }}
          UNION {{
            {0} <http://schema.org/logo> ?img .
          }}
          UNION {{
            {0} <http://xmlns.com/foaf/0.1/depiction> ?img .
          }}
        }} 
    "#,
                object
            )
        };
        let images = self.query(&query_image).unwrap_or(vec![]);
        let mut imgs = vec![];
        for img in images {
            let img_path = extract_literal(img.get("img")).unwrap_or("".to_string());

            imgs.push(img_path);
        }
        imgs
    }

    pub fn get_predicates(&self, otype: &str) -> Vec<String> {
        let query = format!(r#"
SELECT DISTINCT ?p WHERE {{
    ?s a {otype}.
    ?s ?p ?o.
}}
"#);
        let mut res = vec![];
        let query_result = match self.query(&query) {
            Ok(result) => result,
            Err(_) => panic!("get predicates query failed miserably"),
        };
        for r in query_result {
            res.push(r.get("p").unwrap().to_string());
        }
        res
    }

    pub fn get_counts(&self, query: &str, vname: &str) -> Vec<f64> {
        let mut res = vec![];
        let query_result = match self.query(query) {
            Ok(result) => result,
            Err(_) => {
                println!("{query}");
                panic!("Invalid count query")
            }
        };
        for r in query_result {
            let val = match r.get(vname).unwrap() {
                Literal(l) => l.value().parse::<f64>().unwrap(),
                _ => panic!("invalid count query!!"),
            };
            res.push(val);
        }
        res
    }

    pub fn stat_anal_predicates(
        &self,
        otype: &str,
        edge_rank: &HashMap<String, f64>
    ) -> Option<Vec<(String, HashMap<String, f64>)>> {
        let mut data = vec![];
        let mut recalculate = true;
        match
            load_predicate_analysis(
                &format!(
                    "./data/{}/stat_anal/{}",
                    self.dataset,
                    otype.replace("<", "").replace(">", "").replace(":", "_").replace("/", "\\")
                )
            )
        {
            Ok((version, cached_data)) => {
                if version == self.get_history().lines().count() {
                    data = cached_data;
                    recalculate = false;
                    println!("{otype} analysis loaded");
                }
            }
            Err(_) => (),
        }

        if recalculate {
            let overall_count_query = format!(
                r#"
SELECT (COUNT (DISTINCT ?s) as ?cnt)
WHERE {{
        ?s a {otype}.
}}
"#
            );

            let object_count = *self.get_counts(&overall_count_query, "cnt").first().unwrap();

            let predicates = self.get_predicates(otype);
            let plen = predicates.len();
            let filtered_predicates: Vec<_> = predicates
                .iter()
                .filter(|p| { *p != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" })
                .collect();
            data = filtered_predicates
                .par_iter()
                .map(|p| {
                    (p.to_string(), self.stat_anal_single_predicate(otype, p, plen, object_count))
                })
                .filter(|r| { (r.1["uniqueness"] - 1.0).abs() > 0.0000000000000000001 })
                .collect::<Vec<_>>();
            if data.len() == 0 {
                return None;
            }
            for (pred, scores) in data.iter_mut() {
                scores.insert("edge_rank".to_string(), *edge_rank.get(pred).unwrap_or(&0.0));
            }
            normalize_column(&mut data, "entropy");
            normalize_column(&mut data, "quality");

            match
                save_predicate_anlaysis(
                    &format!(
                        "./data/{}/stat_anal/{}",
                        self.dataset,
                        otype.replace("<", "").replace(">", "").replace(":", "_").replace("/", "\\")
                    ),
                    &data,
                    self.get_history().lines().count()
                )
            {
                Ok(_) => println!("{otype} analysis saved"),
                Err(e) => {
                    println!("error caching {otype} analysis: \n{}", e);
                }
            }
        }
        compute_scores(&mut data);

        return Some(data);
    }

    fn stat_anal_single_predicate(
        &self,
        otype: &str,
        predicate: &str,
        total_predicates: usize,
        object_count: f64
    ) -> HashMap<String, f64> {
        //         let overall_count_query = format!(
        //             r#"
        // SELECT (COUNT (DISTINCT ?s) as ?cnt)
        // WHERE {{
        //         ?s a {otype}.
        // }}
        // "#
        //         );
        let frequency_query = format!(
            r#"
SELECT (COUNT(DISTINCT ?s) as ?cnt)
WHERE {{
        ?s a {otype};
        {predicate} ?o.
}}
        "#
        );
        let distinct_objects_query = format!(
            r#"
SELECT (COUNT(DISTINCT ?o) as ?cnt){{
        ?s a {otype};
        {predicate} ?o.
}}        
"#
        );

        let entropy_query = format!(
            r#"
SELECT (COUNT(?s) AS ?cnt) 
WHERE {{
    ?s a {otype}.
    ?s {predicate} ?v.
}} 
GROUP BY ?v
            
            "#
        );
        let used_query = format!(
            r#"
SELECT (COUNT(?o) AS ?cnt) 
WHERE {{
    ?s a {otype}.
    ?s {predicate} ?o.
}}
            
            "#
        );

        let entity_quality_query = format!(
            r#"
        SELECT (COUNT(DISTINCT ?p2) as ?cnt) WHERE {{
            ?s a {otype}.
            ?s {predicate} ?o1.
            ?s ?p2 ?o2.
            FILTER(?p2!={predicate})
        }}
        GROUP BY ?s
        "#
        );

        // let object_count = *self.get_counts(&overall_count_query, "cnt").first().unwrap();
        let predicate_used = *self.get_counts(&frequency_query, "cnt").first().unwrap();
        let distinct_objects = *self.get_counts(&distinct_objects_query, "cnt").first().unwrap();

        let entropy_vals = self.get_counts(&entropy_query, "cnt");
        let total_uses = *self.get_counts(&used_query, "cnt").first().unwrap();

        let mut ent: f64 = 0.0;
        for e in entropy_vals {
            let p = e / total_uses;
            ent -= p * p.log2();
        }

        let entity_quality = self.get_counts(&entity_quality_query, "cnt");
        let mut quality = 0.0;
        for q in entity_quality {
            quality += (total_predicates as f64) / q;
        }
        let mut result = HashMap::new();
        result.insert("frequency".to_string(), predicate_used / object_count);
        result.insert("uniqueness".to_string(), distinct_objects / total_uses);
        result.insert("entropy".to_string(), ent);
        result.insert("quality".to_string(), quality);

        result
    }

    pub fn stat_anal_types(
        &self,
        start_with: &str
    ) -> Vec<(String, (f64, f64, f64, f64, i32, bool, f64))> {
        let (mut graph, mut node_map) = self.calculate_class_relations_graph();
        // let literal = node_map["Literal"];

        // DFS Traversal starting from the main type

        let mut order = remove_disconnected(&mut graph, &mut node_map, start_with.to_string());

        // Calculating probabilities for each node
        let mut node_counts: HashMap<String, f64> = HashMap::new();

        for node in node_map.keys() {
            if node == "Literal" {
                continue;
            }
            let q = format!(
                r#"
            SELECT (COUNT(?s) as ?cnt) WHERE {{
                ?s a {node}.
            }}
            "#
            );
            let cnt = *self.get_counts(&q, "cnt").get(0).unwrap();
            node_counts.insert(node.clone(), cnt);
        }

        let level = 3;

        let mut overall_stats = HashMap::new();

        for i in 0..level {
            calculate_probabilities_for_graph(&mut graph);

            let (fpr, _) = self.page_rank(&graph, &node_map, &node_counts, Outgoing);
            let (rpr, _) = self.page_rank(&graph, &node_map, &node_counts, Incoming);

            let mut stats = vec![];
            for (t, depth) in &order {
                // println!("{}", t);
                stats.push((t.clone(), node_counts[t], 1.0 / (1.0 + depth), fpr[t], rpr[t]));
            }

            let keep = self.rank(&stats, (1.0 + (i as f64)) / ((level as f64) + 1.0));

            for (t, depth) in &order {
                if overall_stats.contains_key(t) {
                    *overall_stats.get_mut(t).unwrap() = (
                        node_counts[t],
                        1.0 / (1.0 + depth),
                        fpr[t],
                        rpr[t],
                        i,
                        keep.contains_key(t),
                        *keep.get(t).unwrap_or(&0.0),
                    );
                } else {
                    overall_stats.insert(t.clone(), (
                        node_counts[t],
                        1.0 / (1.0 + depth),
                        *fpr.get(t).unwrap_or(&0.0),
                        *rpr.get(t).unwrap_or(&0.0),
                        i,
                        keep.contains_key(t),
                        *keep.get(t).unwrap_or(&0.0),
                    ));
                }
            }
            let keys_to_remove: Vec<String> = node_map
                .keys()
                .filter(|key| !(keep.contains_key(*key) || *key == "Literal"))
                .cloned()
                .collect();

            // Sort node indices in descending order to remove from highest index first
            let mut indices_to_remove: Vec<(String, NodeIndex)> = keys_to_remove
                .iter()
                .map(|key| (key.clone(), node_map[key]))
                .collect();
            indices_to_remove.sort_by(|a, b| b.1.index().cmp(&a.1.index()));

            for (_, id) in indices_to_remove {
                graph.remove_node(id);
            }

            // order = remove_disconnected(&mut graph, &mut node_map, start_with.to_string());

            for o in &order {
                println!("{}", o.0);
            }
            order = order
                .iter()
                .filter(|(n, _)| { keep.contains_key(n) })
                .cloned()
                .collect::<Vec<_>>();
            println!("Round {i}");
            node_map.clear();
            for n in graph.node_indices() {
                node_map.insert(graph[n].clone(), n);
                // println!("{}", graph[n]);
            }

            let count_keys_to_remove: Vec<String> = node_counts
                .keys()
                .filter(|key| !node_map.contains_key(*key))
                .cloned()
                .collect();

            for key in count_keys_to_remove {
                node_counts.remove(&key);
            }
        }
        let mut keep = vec![];
        for n in graph.node_indices() {
            if graph[n] != "Literal" {
                keep.push(graph[n].clone());
            }
            println!("{}", graph[n]);
        }
        let mut result = overall_stats
            .iter()
            .map(|a| { (a.0.to_string(), *a.1) })
            .collect::<Vec<_>>();
        result.sort_by(|a, b| { b.1.4.cmp(&a.1.4).then_with(|| b.1.6.total_cmp(&a.1.6)) });

        let mut scores = HashMap::new();
        result.iter().for_each(|(n, (_, _, _, _, _, _, s))| {
            scores.insert(n.to_string(), *s);
        });
        self.keep_types(keep);
        self.fix_types(scores);

        return result;

        // self.keep_types(keep);
    }

    fn rank(&self, stats: &Vec<(String, f64, f64, f64, f64)>, limit: f64) -> HashMap<String, f64> {
        let mut s = 0.0;
        let total_count = stats
            .iter()
            .map(|(_, c, _, _, _)| *c)
            .sum::<f64>();
        let mut scores = stats
            .iter()
            .map(|(node, count, depth, fpr, rpr)| {
                let score = (count / total_count).sqrt().sqrt() * depth.sqrt() * (fpr * 3.0 + rpr);
                s += score.exp();

                (node, score.exp())
            })
            .collect::<Vec<_>>();
        for (_, score) in &mut scores {
            *score = *score / s;
        }

        scores.sort_by(|a, b| { b.1.total_cmp(&a.1) });

        // for s in &scores {
        //     println!("{}: {}", s.0, s.1);
        // }

        let mut results = HashMap::new();
        let mut limit = limit.clone();
        let mut i = 0;
        for (n, s) in &scores {
            if limit <= 0.0 {
                break;
            }
            results.insert(n.to_string(), *s);
            limit -= s;
            i += 1;
        }
        println!("Kept: {i}, Removed: {}", scores.len() - i);
        results
    }
    pub fn keep_types(&self, keep: Vec<String>) {
        let filter = keep.join(",");

        let q = format!(
            "
DELETE {{
    ?s a ?t .
        }}
WHERE {{
    ?s a ?t .
    FILTER( !(
        ?t IN (
        {filter}
        )
    ))
        }}
        
        "
        );

        println!("{}", q);

        match self.update(&q) {
            Ok(_) => {
                self.write_to_history(format!("```sparql\n{}\n```", q));
                match
                    self.execute("general.sparql::Remove entities withot type@advanced".to_string())
                {
                    Ok(_) => println!("Yeah"),
                    Err(_) => println!("Noo"),
                }
            }
            Err(_) => println!("NOOO"),
        };
    }

    pub fn calculate_class_relations_graph(
        &self
    ) -> (Graph<String, (String, f64, Option<f64>, Option<f64>)>, HashMap<String, NodeIndex>) {
        // Graph initialization
        let mut graph: Graph<String, (String, f64, Option<f64>, Option<f64>)> = Graph::new();
        let mut node_map: HashMap<String, NodeIndex> = HashMap::new();
        node_map.insert("Literal".to_string(), graph.add_node("Literal".to_string()));
        let mut adj_list = vec![];

        // Checking for a cached version
        let mut recalculate = false;
        match load_relations(&format!("./data/{}.db/relation_counts", self.dataset.to_lowercase())) {
            Ok((version, result)) => {
                if version == self.get_history().lines().count() {
                    adj_list = result;
                } else {
                    recalculate = true;
                }
            }
            Err(_) => {
                recalculate = true;
            }
        }

        // Slower when cached, but acceptable
        let classes_query = "SELECT DISTINCT ?t WHERE {
            ?s a ?t.
        }";
        let types = match self.query(classes_query) {
            Ok(result) =>
                result
                    .iter()
                    .map(|sol| { sol.get("t").unwrap().to_string() })
                    .collect::<Vec<_>>(),
            Err(_) => panic!("Failed to fetch types. Failed miserably"),
        };

        //Doing the computation if no cached version
        if recalculate {
            let _ = self.execute("class_graph.sparql::Clear class relations graph".to_string());
            for t in types {
                let nid = graph.add_node(t.clone());
                node_map.insert(t.clone(), nid);

                let outgoing_edges_query = format!(
                    r#"
SELECT ?p ?t2 (COUNT(?o) as ?cnt) WHERE {{
    ?s ?p ?o.
    ?s a {t}.
    OPTIONAL {{?o a ?t2}}
}}
GROUP BY ?p ?t2
            "#
                );
                match self.query(&outgoing_edges_query) {
                    Ok(result) =>
                        result.iter().for_each(|r| {
                            let itm = (
                                t.clone(),
                                r.get("p").unwrap().to_string(),
                                match r.get("t2") {
                                    Some(v) => v.to_string(),
                                    None => "Literal".to_string(),
                                },
                                match r.get("cnt").unwrap() {
                                    Literal(literal) => literal.value().parse::<f64>().unwrap(),
                                    _ => panic!("Count is not a literal!!! Not possible"),
                                },
                            );
                            // Keeping legacy class graph in the store
                            if itm.2 != "Literal".to_string() {
                                let q = &format!(
                                    r#"
INSERT DATA {{   
    GRAPH <urn:class_relations> {{
        {} {} {}.
    }}
}}"#,
                                    itm.0,
                                    itm.1,
                                    itm.2
                                );
                                match self.update(q) {
                                    Ok(_) => (),
                                    Err(e) =>
                                        match e {
                                            StoreError::EvaluationError(err) => {
                                                println!("{}", q);
                                                println!("{}", err);
                                            }
                                            StoreError::UnsupportedError => (),
                                        }
                                }
                            }
                            if !(itm.1 == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
                                adj_list.push(itm);
                            }
                        }),
                    Err(_) => panic!("Something went wronnnnng!"),
                };
            }
            match
                save_relations(
                    &format!("./data/{}.db/relation_counts", self.dataset.to_lowercase()),
                    &adj_list,
                    self.get_history().lines().count()
                )
            {
                Ok(_) => println!("class graph saved"),
                Err(_) => println!("error caching class graph"),
            };
        } else {
            for t in types {
                let nid = graph.add_node(t.clone());
                node_map.insert(t.clone(), nid);
            }
        }

        // Loading to a graph from the adjecency list
        for e in adj_list {
            graph.add_edge(node_map[&e.0], node_map[&e.2], (e.1, e.3, None, None));
        }
        (graph, node_map)
    }

    pub fn page_rank(
        &self,
        graph: &Graph<String, (String, f64, Option<f64>, Option<f64>)>,
        node_map: &HashMap<String, NodeIndex>,
        node_counts: &HashMap<String, f64>,
        direction: petgraph::Direction
    ) -> (HashMap<String, f64>, HashMap<String, HashMap<String, f64>>) {
        let mut page_rank = HashMap::new();
        let mut edge_rank = HashMap::new();
        let mut ertotal = 0.0;
        let mut total = 0.0;

        let literal = node_map["Literal"];

        for n in node_map.keys() {
            page_rank.insert(n.clone(), 0.0);
        }

        for _ in 0..10000 {
            let node = choice(&node_counts).unwrap();
            let mut current = *node_map.get(&node).unwrap();
            *page_rank.get_mut(&node).unwrap() += 1.0;
            total += 1.0;

            for _ in 0..10 {
                let neighbors = graph.edges_directed(current, direction);
                let mut edge_map: HashMap<EdgeIndex, f64> = HashMap::new();
                for edge in neighbors {
                    let id = edge.id();
                    if direction == Outgoing {
                        edge_map.insert(id, edge.weight().2.unwrap());
                    } else {
                        edge_map.insert(id, edge.weight().3.unwrap());
                    }
                }
                if edge_map.is_empty() {
                    break;
                }
                let follow = choice(&edge_map).unwrap();
                let key = (graph[current].clone(), graph.edge_weight(follow).unwrap().0.clone());
                if !edge_rank.contains_key(&key.0) {
                    edge_rank.insert(key.0.clone(), HashMap::new());
                }
                if edge_rank.get(&key.0).unwrap().contains_key(&key.1) {
                    *edge_rank.get_mut(&key.0).unwrap().get_mut(&key.1).unwrap() += 1.0;
                } else {
                    edge_rank.get_mut(&key.0).unwrap().insert(key.1, 1.0);
                }
                ertotal += 1.0;
                let last = current.clone();
                if direction == Outgoing {
                    current = graph.edge_endpoints(follow).unwrap().1;
                } else {
                    current = graph.edge_endpoints(follow).unwrap().0;
                }
                if current == literal {
                    *page_rank.get_mut(&graph[last]).unwrap() += graph
                        .edge_weight(follow)
                        .unwrap()
                        .2.unwrap();
                    break;
                }
                *page_rank.get_mut(&graph[current]).unwrap() += 1.0;
                total += 1.0;
            }
        }
        for (_, v) in &mut page_rank {
            *v /= total;
        }
        for (_, hm) in &mut edge_rank {
            for (_, v) in hm.iter_mut() {
                *v /= ertotal;
            }
        }
        (page_rank, edge_rank)
    }

    fn fix_types(&self, scores: HashMap<String, f64>) {
        let q =
            "
        SELECT DISTINCT ?t1 ?t2  {{
            ?s a ?t1.
            ?s a ?t2.
            FILTER (?t1!=?t2)
        }}
        LIMIT 1
        ";

        loop {
            match self.query(&q) {
                Ok(result) => {
                    if result.is_empty() {
                        break;
                    }
                    let r = result.get(0).unwrap();
                    let t1 = r.get("t1").unwrap().to_string();
                    let t2 = r.get("t2").unwrap().to_string();
                    let (keep, skip) = if scores[&t1] > scores[&t2] { (t1, t2) } else { (t2, t1) };
                    let query = format!(
                        r#"
                        DELETE {{
                            ?s a {skip}.
                        }}
                        INSERT {{
                            ?s <http://schema.org/additionaltype> {skip}.
                        }}
                        WHERE {{
                            ?s a {skip}.
                            ?s a {keep}.
                        }}
                    
                    "#
                    );
                    match self.update(&query) {
                        Ok(_) => {
                            self.write_to_history(format!("```sparql\n{}\n```", query));
                        }
                        Err(_) => {
                            panic!("ERROR");
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
    pub fn delete_predicate(&self, otype: &str, pred: &str) {
        let q = format!(
            r#"
            DELETE {{
                ?s {pred} ?pval.
            }}
            WHERE {{
                ?s a {otype}.
                ?s {pred} ?pval.
            }}
        
        "#
        );
        match self.update(&q) {
            Ok(_) => {
                self.write_to_history(format!("```sparql\n{}\n```", q));
            }
            Err(_) => panic!("failed to delete predicate {pred} for type {otype}"),
        }
    }

    pub fn analyse_objects(&self, otype: &str) -> i64 {
        let mut cnt = 0;
        let mut scores = HashMap::new();
        match
            load_predicate_analysis(
                &format!(
                    "./data/{}/stat_anal/{}",
                    self.dataset,
                    otype.replace("<", "").replace(">", "").replace(":", "_").replace("/", "\\")
                )
            )
        {
            Ok((_, mut data)) => {
                compute_scores(&mut data);
                data.iter().for_each(|(k, v)| {
                    scores.insert(k.clone(), v.get("score").unwrap().clone());
                });
            }
            Err(_) => (),
        }
        let mut sm = 0.0;
        for (_, s) in &scores {
            sm += s;
        }
        sm = sm / 2.0;

        let q = format!(r#"
        SELECT ?s {{
            ?s a {otype}
        }}
        "#);

        match self.query(&q) {
            Ok(result) => {
                for r in result {
                    let s = r.get("s").unwrap();
                    let qs = format!(
                        r#"
                        SELECT DISTINCT ?p WHERE {{
                            {s} ?p ?v.
                        }}
                    "#
                    );
                    let preds = match self.query(&qs) {
                        Ok(r) => {
                            r.iter()
                                .map(|sol| { sol.get("p").unwrap().to_string() })
                                .collect::<Vec<_>>()
                        }
                        Err(_) => vec![],
                    };
                    let mut score = 0.0;
                    for p in preds {
                        score += scores.get(&p).unwrap_or(&0.0);
                    }
                    if score > sm {
                        cnt += 1;
                    }
                }
            }
            Err(_) => panic!("Failed to analyse objects of type {otype}"),
        }
        cnt
    }
}

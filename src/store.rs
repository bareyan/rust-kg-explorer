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
use oxigraph::store::Store;
use oxigraph::sparql::{ QueryResults, QuerySolution };
use oxigraph::io::{ RdfParser, RdfFormat };

// Create imports
use crate::utils::{ self, extract_literal };
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

    /// # Loading procedures

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

    /// # Getters

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

    /// # History

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

    /// # Store operations

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

    /// # Version management

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
    pub fn replay_history(&self, content: String) -> Result<(), StoreError> {
        let lines = content.lines().map(str::trim);
        let mut in_sparql = false;
        let mut sparql_block = String::new();

        for line in lines {
            if line.starts_with("```sparql") {
                in_sparql = true;
                sparql_block.clear();
            } else if line.starts_with("```") && in_sparql {
                in_sparql = false;
                // Execute the SPARQL block
                if sparql_block.contains("#\n") {
                    // Advanced query with select and update parts
                    let parts: Vec<&str> = sparql_block.split("#\n").collect();
                    if parts.len() == 2 {
                        let (select_query, update_query) = (parts[0].trim(), parts[1].trim());
                        self.iterative_update(select_query, update_query)?;
                    } else {
                        // Fallback to regular update if format is unexpected
                        self.update(&sparql_block)?;
                    }
                } else {
                    // Regular SPARQL update
                    self.update(&sparql_block)?;
                }
            } else if in_sparql {
                sparql_block.push_str(line);
                sparql_block.push('\n');
            } else if line.contains("::") && !line.starts_with("Dumping") {
                // This is a routine execution line
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
                                        self.iterative_update(select_query, update_query)?;
                                    } else {
                                        self.update(&current_query)?;
                                    }
                                } else {
                                    self.update(&current_query)?;
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

                    // Handle case where procedure is at the end of file
                    if in_proc && current_name == proc {
                        if is_advanced {
                            let parts: Vec<&str> = current_query.split("#\n").collect();
                            if parts.len() == 2 {
                                let (select_query, update_query) = (
                                    parts[0].trim(),
                                    parts[1].trim(),
                                );
                                self.iterative_update(select_query, update_query)?;
                            } else {
                                self.update(&current_query)?;
                            }
                        } else {
                            self.update(&current_query)?;
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

    /// # Useful procedures

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

    /// Fetches detailed information for an entity given as a string IRI.
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
}

use core::panic;
use std::fs::File;
use std::io::{ Write };
use std::str::FromStr;
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::sync::Arc;
use std::time::Instant;
use flate2::read::GzDecoder;
use std::vec;
use std::path::Path;
use oxigraph::model::{ NamedNode, Term };
use oxigraph::store::Store;
use oxigraph::sparql::{ QueryResults, QuerySolution };
use oxigraph::io::{ RdfParser, RdfFormat };
use item::Item;

use crate::utils::{ self, extract_literal, verify_valid };
use crate::item;

pub enum StoreError {
    EvaluationError(String),
    UnsupportedError,
}

pub struct KG {
    pub dataset: String,
    download_path: String,
    nb_parts: u32,
    pub store: Option<Store>,
}

impl KG {
    pub fn new(dataset_name: &str, nb_parts: u32) -> KG {
        let mut created = KG {
            dataset: dataset_name.to_string(),
            download_path: format!("https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/{}/part_", dataset_name),
            nb_parts,
            store: None,
        };
        //Check if the store is not yet created and download the dataset if needed
        if !Path::new(&format!("./data/{}.db", dataset_name.to_lowercase())).exists() {
            created.download_dataset();
        }
        created.load();
        created
    }
    pub fn from_file(dataset_path: &str) -> KG {
        let mut created = KG {
            dataset: dataset_path.to_string(),
            download_path: String::new(),
            nb_parts: 1,
            store: None,
        };
        created.load_file(dataset_path);
        created
    }
    fn download_dataset(&self) {
        let mut now = Instant::now();

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
                let url = format!("{}{}.gz", self.download_path, i);
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
    fn load_file(&mut self, file_path: &str) {
        let filename = match file_path.split("/").last() {
            Some(f) => f,
            None => panic!("Invalid file path"),
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
    }
    fn load(&mut self) {
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
    }

    pub fn get_objects(
        &self,
        object_type: &str,
        limit: u32,
        offset: u32
    ) -> Vec<oxigraph::model::Term> {
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

    pub fn get_info(&self, object: oxigraph::model::Term) -> Item {
        match object {
            Term::NamedNode(named_node) => {
                let type_query =
                    format!("
                    SELECT ?otype WHERE {{
                        {} a ?otype .
                    }}
                ", named_node);

                let name_query =
                    format!("
                    SELECT ?name WHERE {{
                        {} <http://schema.org/name> ?name .
                    }}
                    LIMIT 1
                ", named_node);

                let description_query =
                    format!("
                    SELECT ?description WHERE {{
                        {} <http://schema.org/description> ?description .
                    
                    }}
                    LIMIT 1
                ", named_node);
                let typer = self.query(&type_query).unwrap_or(vec![]);
                let namer = self.query(&name_query).unwrap_or(vec![]);
                let descriptionr = self.query(&description_query).unwrap_or(vec![]);

                let otype = if typer.is_empty() {
                    None
                } else {
                    typer.first().unwrap().get("otype")
                };
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

                let images = self.get_images(&named_node.to_string());
                Item::new(
                    named_node.into(),
                    vec![otype.cloned().unwrap()],
                    name,
                    description,
                    images
                )
            }
            _ => panic!("Invalid argument"),
        }
    }

    pub fn details(&self, object: &str) -> Item {
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
        Item::new(node.into(), otypes, name, description, self.get_images(object))
    }

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

    pub fn get_images(&self, object: &str) -> Vec<String> {
        let query_image = format!(
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
        );
        let images = self.query(&query_image).unwrap_or(vec![]);
        let mut imgs = vec![];
        for img in images {
            let img_path = extract_literal(img.get("img")).unwrap_or("".to_string());
            if verify_valid(&img_path) {
                imgs.push(img_path);
            }
        }
        imgs
    }
}

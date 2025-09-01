//! Utility module for RDF data processing and web interface formatting.
//!
//! This module provides functions for:
//! - Preprocessing N-Quads files into N-Triples format
//! - Converting blank nodes to skolemized IRIs
//! - Extracting values from RDF terms
//! - HTML and JavaScript escaping for web output
//! - Creating hyperlinks from RDF resources
//! - Formatting query results as JSON for visualization
//! - URL decoding for web parameters

use std::collections::{ HashMap, HashSet };
use std::fs::File;
use std::io::{ BufWriter, Write, BufReader, BufRead };
use std::option::Option;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction::Outgoing;
use regex::{ Match, Regex };

// use serde::{ Serialize, Deserialize };
// Oxigraph
use oxigraph::model::Term::{ self, NamedNode, Literal };
use oxigraph::sparql::{ QuerySolution };
use rand::Rng;

/// Preprocesses an N-Quads file by performing a series of normalization and cleanup steps:
/// - Removes invalid Unicode replacement characters and standardizes schema.org IRIs.
/// - Converts inline JSON‐LD constructs wrapped in `<…{…}…>` into quoted literals.
/// - Rewrites `<@type:>` tokens to the standard RDF type IRI.
/// - Strips named graph annotations, ending each triple with a simple `.`.
/// - Skolemizes blank node labels into unique IRIs.
/// - Writes each cleaned line into a new `.nt` file with the same base name.
///
/// # Arguments
///
/// * `nquads_file` – Path to the input N-Quads file. The output will be saved as
///   `{nquads_file}.nt`.
///
/// # Panics
///
/// This function will panic if the input file cannot be opened, the output file cannot
/// be created, or if any I/O error occurs during processing.
pub fn preprocess(nquads_file: &str) {
    let infile = File::open(nquads_file).unwrap();
    let outfile = File::create(format!("{}.nt", nquads_file)).unwrap();

    let mut reader = BufReader::new(infile);
    let mut writer = BufWriter::new(outfile);
    let mut line = String::new();

    let re = Regex::new(r"<[^>]+\{(.*)\}[^>]*>").unwrap();
    let iri = Regex::new(r"<([^>]+)>").unwrap();
    let graph_name = Regex::new(r"<([^>]*)>\s*\.\n").unwrap();
    let bnode_regex = Regex::new(r"_:([A-Za-z0-9]+)").unwrap();

    let schema = Regex::new(r"<https?:\/\/schema\.org\/([^>]*)>").unwrap();

    while reader.read_line(&mut line).expect("Failed to read the file") != 0 {
        line = line.replace("\\uFFFD", "");

        line = schema
            .replace_all(&line, |caps: &regex::Captures| {
                format!("<http://schema.org/{}>", caps.get(1).unwrap().as_str().to_lowercase())
            })
            .into_owned();

        //Book specific fix
        if
            line.contains("https://www.deutscherkunstverlag.de/") &&
            line.contains("schema.org/url>")
        {
            let ms = iri.find_iter(&line).collect::<Vec<Match>>();
            if ms[ms.len() - 2].as_str() != ms[ms.len() - 1].as_str() {
                line = line.replace(ms[ms.len() - 2].as_str(), ms[ms.len() - 1].as_str());
            }
        }

        if line.contains("}") {
            match re.find(&line) {
                Some(m) => {
                    line = line.replace(
                        m.as_str(),
                        &m.as_str().replace("<", "\"").replace(">", "\"")
                    );
                }
                None => (),
            }
        }
        if line.contains("<@type:>") {
            line = line.replace("@type:", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        }

        line = graph_name.replace(&line, ".").to_string();
        line = bnode_regex
            .replace_all(&line, |caps: &regex::Captures| {
                skolemize(caps.get(0).unwrap().as_str().to_string())
            })
            .into_owned();

        let _ = writeln!(writer, "{}", line);
        line.clear();
    }
}

/// Skolemizes a blank node identifier into a URN‐style IRI.
///
/// # Arguments
///
/// * `blank_node` – The blank node label (e.g. `_:b0`).
///
/// # Returns
///
/// A string containing a skolem IRI (e.g. `<urn:skolem:b0>`).
pub fn skolemize(blank_node: String) -> String {
    return format!("<{}>", blank_node.replace("_", "urn:skolem"));
}

/// Extracts the string value from an `Option<&Term>`, handling literals and named nodes.
///
/// # Arguments
///
/// * `term` – An optional reference to an Oxigraph `Term`.
///
/// # Returns
///
/// * `Some(String)` with the literal value or IRI (angle brackets stripped), or `None` for other term types or `None` input.
pub fn extract_literal(term: Option<&Term>) -> Option<String> {
    match term {
        Some(t) => {
            match t {
                Literal(l) => Some(l.value().to_string()),
                NamedNode(nnode) => Some(nnode.to_string().replace("<", "").replace(">", "")),
                _ => None,
            }
        }
        None => None,
    }
}

/// Decodes a percent-encoded string, converting `+` to space and `%HH` to their byte value.
///
/// # Arguments
///
/// * `input` – The percent-encoded input string.
///
/// # Returns
///
/// The decoded `String`.
pub fn url_decode(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '+' => result.push(' '),
            '%' => {
                if let (Some(h1), Some(h2)) = (chars.next(), chars.next()) {
                    let hex = format!("{}{}", h1, h2);
                    if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                        result.push(byte as char);
                    }
                }
            }
            _ => result.push(c),
        }
    }

    result
}

/// Retrieves a query parameter value from a URL-encoded query string.
///
/// # Arguments
///
/// * `query` – The raw query string (e.g. `"a=1&b=2"`).
/// * `key` – The parameter name to extract.
///
/// # Returns
///
/// * `Some(String)` of the decoded parameter value, or `None` if the key is not present.
pub fn extract_query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let p = percent_encoding::percent_decode_str(pair).decode_utf8().unwrap().to_string();
        let mut parts = p.splitn(2, '=');
        let k = parts.next()?;
        let v = parts.next()?;
        if k == key {
            return Some(v.replace("+", " ").replace("%20", " ").replace("%23", ""));
        }
    }
    None
}

/// Escapes HTML-special characters in a string to their entity equivalents.
///
/// # Arguments
///
/// * `data` – The input string to escape.
///
/// # Returns
///
/// The escaped HTML string.
pub fn escape_html(data: &String) -> String {
    data.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}

/// Escapes characters in a string for safe embedding in JavaScript.petgraph::graph::Graph<String, (String, )>
///
/// # Arguments
///
/// * `data` – The input string to escape.
///
/// # Returns
///
/// The escaped JavaScript string.
pub fn escape_js(data: String) -> String {
    data.replace("'", "\\'").replace("\"", "\\\"").replace("\n", "\\n").replace("\t", "\\t")
}

/// Renders an internal entity IRI as an HTML link to the entities page.
///
/// # Arguments
///
/// * `data` – A string containing an encoded IRI (e.g. `&lt;http://…&gt;`).
///
/// # Returns
///
/// An `<a>` tag linking to `/entity/{IRI}` or the original data if not an IRI.
pub fn to_link(data: String) -> String {
    if data.starts_with("&lt;") && data.ends_with("&gt;") {
        let link = data.replace("&lt;", "/entity/<").replace("&gt;", ">").replace("#", "%23");
        format!("<a href=\"{}\">{}</a>", link, data)
    } else {
        data
    }
}

/// Renders any IRI as an external HTML link, escaping HTML characters.
///
/// # Arguments
///
/// * `data` – A string possibly containing `<http…>`.
///
/// # Returns
///
/// An `<a>` tag linking to the external resource, or the escaped input if not an IRI.
pub fn external_link(data: &str) -> String {
    if data.starts_with("<http") {
        let d = data.replace("<", "").replace(">", "");
        return format!("<a href=\"{d}\">{}</a>", escape_html(&data.to_string()));
    }
    escape_html(&data.to_string())
}

/// Formats a list of SPARQL query solutions for a given entity into a JSON‐compatible string.
///
/// # Arguments
///
/// * `entity` – The entity IRI to describe.
/// * `props` – A vector of `QuerySolution` objects with `predicate` and `object` bindings.
///
/// # Returns
///
/// A string containing a JSON fragment with id, name, optional image, URL, and attribute pairs.
pub fn format_json(entity: String, props: Vec<QuerySolution>) -> String {
    let mut name = entity.clone();
    let mut inside = String::new();

    let mut is_img = false;
    for connection in props {
        let p = connection.get("predicate").unwrap().to_string();
        let o = connection.get("object").unwrap();

        // Handle special predicates
        match (p.as_str(), o) {
            ("<http://schema.org/name>", Literal(lit)) => {
                name = escape_js(lit.value().to_string());
            }
            ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", NamedNode(named)) => {
                if named.to_string() == "<http://schema.org/ImageObject>" {
                    is_img = true;
                }
            }
            _ => {}
        }
        inside += &format!(
            "[\"{}\",  \'{}\'],",
            escape_js(
                escape_html(
                    &p
                        .replace("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "type")
                        .replace("<http://schema.org/", ":")
                        .replace(">", "")
                )
            ),
            escape_js(escape_html(&o.to_string()))
        );
    }

    let imagerow = if is_img {
        format!("image: \"{}\",", entity.replace("<", "").replace(">", ""))
    } else {
        String::new()
    };
    format!(
        r#"
        {{
            id: "{entity}",
            name: "{name}",
            {imagerow}
            url: "/entity/{entity}",
            attributes: [
                {inside}
            ]
    }},
    
    "#
    )
}

pub fn calculate_probabilities_for_graph(
    graph: &mut petgraph::Graph<String, (String, f64, Option<f64>, Option<f64>)>
) {
    for n in graph.node_indices() {
        let mut s = 0.0;

        for e in graph.edges_directed(n, petgraph::Direction::Outgoing) {
            let (_, cnt, _, _) = e.weight();
            s += cnt;
        }

        let outgoing_edges: Vec<_> = graph
            .edges_directed(n, petgraph::Direction::Outgoing)
            .map(|edge| {
                let edge_id = edge.id();
                let (name, cnt, _, _) = graph.edge_weight(edge_id).unwrap();
                (edge_id, name.clone(), *cnt)
            })
            .collect();

        for (edge_id, name, cnt) in outgoing_edges {
            *graph.edge_weight_mut(edge_id).unwrap() = (name.clone(), cnt, Some(cnt / s), None);
        }
    }
    for n in graph.node_indices() {
        let mut s2 = 0.0;

        for e in graph.edges_directed(n, petgraph::Direction::Incoming) {
            let (_, cnt, _, _) = e.weight();
            s2 += cnt;
        }

        let incoming_edges: Vec<_> = graph
            .edges_directed(n, petgraph::Direction::Incoming)
            .map(|edge| {
                let edge_id = edge.id();
                let (name, cnt, pr, _) = graph.edge_weight(edge_id).unwrap();
                (edge_id, name.clone(), *cnt, *pr)
            })
            .collect();

        for (edge_id, name, cnt, pr) in incoming_edges {
            *graph.edge_weight_mut(edge_id).unwrap() = (name, cnt, pr, Some(cnt / s2));
        }
    }
}

pub fn choice<T: Clone>(map: &HashMap<T, f64>) -> Option<T> {
    if map.is_empty() {
        println!("EMPTY MAP ");
        return None;
    }

    let total: f64 = map.values().sum();
    if total == 0.0 {
        println!("NUL TOTAL");
        return None;
    }

    let mut rng = rand::rng();
    let mut rand: f64 = rng.random();
    for (key, weight) in map {
        if rand < *weight / total {
            return Some(key.clone());
        }
        rand -= weight / total;
    }
    None
}

// #[derive(Serialize, Deserialize, Debug)]
// struct RelationCounts(String, String, String, f64);

pub fn save_relations(
    path: &str,
    data: &Vec<(String, String, String, f64)>,
    version: usize
) -> std::io::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, &(version, data))?;
    Ok(())
}

pub fn load_relations(path: &str) -> std::io::Result<(usize, Vec<(String, String, String, f64)>)> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let data = serde_json::from_reader(reader)?;
    Ok(data)
}

pub fn save_predicate_anlaysis(
    path: &str,
    data: &Vec<(String, HashMap<String, f64>)>,
    version: usize
) -> std::io::Result<()> {
    println!("{}", path);
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, &(version, data))?;
    Ok(())
}

pub fn load_predicate_analysis(
    path: &str
) -> std::io::Result<(usize, Vec<(String, HashMap<String, f64>)>)> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let data = serde_json::from_reader(reader)?;
    Ok(data)
}

pub fn normalize_column(data: &mut Vec<(String, HashMap<String, f64>)>, col: &str) {
    let min_val = data
        .iter()
        .map(|(_, row)| row[col])
        .fold(f64::INFINITY, f64::min);
    let max_val = data
        .iter()
        .map(|(_, row)| row[col])
        .fold(f64::NEG_INFINITY, f64::max);

    if (max_val - min_val).abs() < 1e-12 {
        // avoid divide by zero if column constant
        for (_, row) in data.iter_mut() {
            *row.get_mut(col).unwrap() = 0.0;
        }
    } else {
        for (_, row) in data.iter_mut() {
            *row.get_mut(col).unwrap() = (row[col] - min_val) / (max_val - min_val);
        }
    }
}

pub fn compute_scores(data: &mut Vec<(String, HashMap<String, f64>)>) {
    let mut softmax_sum = 0.0;
    let inv_temp = data.len() as f64;
    let mut s = 0.0;
    for (name, row) in data.iter_mut() {
        let f = row["frequency"]; // frequency
        let u = row["uniqueness"]; // uniqueness
        let h = row["entropy"]; // entropy
        let q = row["quality"]; // entity quality
        let r = row["edge_rank"]; // edge rank

        // log scaling for large values
        // let q_scaled = q.sqrt();
        // let q_scaled = (1.0 - (q - 1.0).powi(2)).sqrt();
        let r_scaled = (1.0 + r).ln();

        let structural = f.sqrt() * q * r_scaled;
        let data_based = h * u;

        // final score
        let score = structural * data_based;
        if score != 0.0 {
            softmax_sum += (score * inv_temp).exp();
            s += score;
            row.insert("score".to_string(), (score * inv_temp).exp());
        } else {
            row.insert("score".to_string(), 0.0);
        }
        println!("{}", name);
        let nn_keep = nn_interface(f, u, h, q, r);
        row.insert("keep".to_string(), nn_keep);
        // if nn_keep > 0.5 {
        // } else {
        //     row.insert("keep".to_string(), 1.0 - nn_keep);
        // }
    }

    println!("{}", s / inv_temp);
    for (_, row) in data.iter_mut() {
        if *row.get("score").unwrap() == 0.0 {
            continue;
        }
        *row.get_mut("score").unwrap() /= softmax_sum / 100.0;
    }
    data.sort_by(|a, b| { b.1["score"].total_cmp(&a.1["score"]) });
    for i in 0..data.len() - 1 {
        let ratio = data[i + 1].1["score"] / data[i].1["score"];
        data[i].1.insert("score_ratio".to_string(), ratio);
    }
    // data[data.len()-1].1.insert(k, v)
}

pub fn remove_disconnected(
    graph: &mut petgraph::Graph<String, (String, f64, Option<f64>, Option<f64>)>,
    node_map: &mut HashMap<String, NodeIndex>,
    start_with: String
) -> Vec<(String, f64)> {
    let literal = node_map["Literal"];
    let mut seen: HashSet<String> = HashSet::new();
    // seen.insert("Literal".to_string());
    let mut items = vec![(format!("<http://schema.org/{}>", start_with), 0.0)];
    let mut order = vec![];

    while items.len() > 0 {
        let (ent, depth) = items.get(0).unwrap().clone();
        if seen.contains(&ent) {
            items.remove(0);
            continue;
        }
        seen.insert(ent.clone());
        order.push((ent.clone(), depth));

        for n in graph.neighbors_directed(node_map[&ent], Outgoing) {
            let new = graph[n].clone();
            if n != literal {
                items.push((new, depth + 1.0));
            }
        }
        items.remove(0);
    }

    order.reverse();

    // for n in graph.node_indices() {
    //     if !seen.contains(&graph[n]) && graph[n] != "Literal" {
    //         node_map.remove(&graph[n]);
    //         graph.remove_node(n);
    //     }
    // }
    return order;
}

use tract_onnx::prelude::*;

type Model = RunnableModel<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>;

pub fn nn_interface(freq: f64, uniqueness: f64, entropy: f64, quality: f64, edge_rank: f64) -> f64 {
    let model: Model = tract_onnx
        ::onnx()
        .model_for_path("./ml/model.onnx")
        .unwrap()
        .with_input_fact(0, f32::fact(&[1, 5]).into())
        .unwrap()
        .into_optimized()
        .unwrap()
        .into_runnable()
        .unwrap();

    // println!("Model loaded successfully.");

    let input_vec = [
        freq as f32,
        uniqueness as f32,
        entropy as f32,
        quality as f32,
        edge_rank as f32,
    ];
    let input: Tensor = tract_ndarray::arr1(&input_vec).to_shape((1, 5)).unwrap().to_owned().into();

    // println!("Running inference with input: {:?}", input_vec);

    let result = model.run(tvec!(input.into())).unwrap();

    let output: &[f32] = result[0].as_slice().unwrap();

    println!("Model output: {:?}", output);
    return output[1] as f64;
}

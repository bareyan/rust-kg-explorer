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

use std::fs::File;
use std::io::{ BufWriter, Write, BufReader, BufRead };
use regex::{ Match, Regex };

// Oxigraph
use oxigraph::model::{ Term };
use oxigraph::model::Term::{ NamedNode, Literal };
use oxigraph::sparql::{ QuerySolution };

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

    while reader.read_line(&mut line).expect("Failed to read the file") != 0 {
        line = line.replace("\\uFFFD", "").replace("https://schema.org", "http://schema.org");

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

/// Escapes characters in a string for safe embedding in JavaScript.
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

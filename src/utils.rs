use std::fs::File;
use std::io::{ BufWriter, Write };
use oxigraph::model::{ Term };
use oxigraph::model::Term::{ NamedNode, Literal };
use oxigraph::sparql::{ QueryResults, QuerySolution };
use oxigraph::store::Store;
use regex::{ Match, Regex };
use url::Url;
use std::io::{ BufRead, BufReader };

pub(crate) fn preprocess(nquads_file: &str) {
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

pub(crate) fn cleanup(store: &Store) {
    println!("Starting cleanup...");
    let updates = vec![
        r#"
        DELETE {
            ?s <http://schema.org/url> ?url.
        }
        INSERT {
            ?s <http://schema.org/url> ?newIRI.
        }
        WHERE {
            ?s a <http://schema.org/ImageObject>.
            ?s <http://schema.org/url> ?url.
            FILTER(isLiteral(?url))
            BIND (IRI(STR(?url)) AS ?newIRI)
        }
        "#,
        r#"
        DELETE {
            ?sub ?p ?s.
        }
        INSERT {
            ?sub ?p ?url.
        }
        WHERE {
            ?s a <http://schema.org/ImageObject>.
            ?s <http://schema.org/url> ?url.
            ?sub ?p ?s.
        }
        "#,
        r#"
        DELETE {
            ?s ?p ?o.
        }
        INSERT {
            ?url ?p ?o.
        }
        WHERE {
            ?s a <http://schema.org/ImageObject>.
            ?s <http://schema.org/url> ?url.
            ?s ?p ?o.
        }
        "#,
        r#"
        DELETE {
            ?s <http://schema.org/url> ?url.
        }
        INSERT {
            ?s <http://schema.org/url> ?newIRI.
        }
        WHERE {
            ?s <http://schema.org/image> ?url.
            FILTER(isLiteral(?url))
            BIND (IRI(STR(?url)) AS ?newIRI)
        }
        "#,
        r#"
        DELETE {
            ?s <http://schema.org/url> ?url.
        }
        INSERT {
            ?s <http://schema.org/url> ?newIRI.
        }
        WHERE {
            ?s <http://schema.org/photo> ?url.
            FILTER(isLiteral(?url))
            BIND (IRI(STR(?url)) AS ?newIRI)
        }
        "#,
        r#"
        DELETE {
            ?s <http://schema.org/url> ?url.
        }
        INSERT {
            ?s <http://schema.org/url> ?newIRI.
        }
        WHERE {
            ?s <http://schema.org/logo> ?url.
            FILTER(isLiteral(?url))
            BIND (IRI(STR(?url)) AS ?newIRI)
        }
        "#,
        r#"
        INSERT {
            ?url a <http://schema.org/ImageObject>.
            ?url <http://schema.org/url> ?url.
        }
        WHERE {
            ?s <http://schema.org/logo> ?url.
            FILTER NOT EXISTS {
                ?url ?p ?o.
            }
        }
        "#,
        r#"
        INSERT {
            ?url a <http://schema.org/ImageObject>.
            ?url <http://schema.org/url> ?url.
        }
        WHERE {
            ?s <http://schema.org/image> ?url.
            FILTER NOT EXISTS {
                ?url ?p ?o.
            }
        }
        "#,
        r#"
        INSERT {
            ?url a <http://schema.org/ImageObject>.
            ?url <http://schema.org/url> ?url.
        }
        WHERE {
            ?s <http://schema.org/photo> ?url.
            FILTER NOT EXISTS {
                ?url ?p ?o.
            }
        }
        "#,
        r#"
        PREFIX schema: <http://schema.org/>
        DELETE {
            ?sub ?p ?s .
        }
        INSERT{
            ?sub ?p ?url.
        }
        WHERE {
            ?s schema:url ?url .
            FILTER (strstarts(str(?s), "urn:skolem")) .
            FILTER NOT EXISTS {
                ?s schema:url ?url2 .
                FILTER(?url != ?url2)
            }
            ?sub ?p ?s.
        }
        "#,
        r#"
        PREFIX schema: <http://schema.org/>
        DELETE {
            ?s ?p ?o.
        }
        INSERT{
            ?url ?p ?o.
        }
        WHERE {
            ?s schema:url ?url .
            FILTER (strstarts(str(?s), "urn:skolem")) .
            FILTER NOT EXISTS {
                ?s schema:url ?url2 .
                FILTER(?url != ?url2)
            }
            ?s ?p ?o.
        }
        "#,
        r#"
        DELETE {
            ?s <http://schema.org/item> ?o.
        }
        INSERT {
            ?s <http://schema.org/item> ?newIRI.
        }
        WHERE {
            ?s <http://schema.org/item> ?o.
            FILTER(isLiteral(?o))
            BIND (IRI(STR(?o)) AS ?newIRI)
        }
        "#,
        r#"
        DELETE {
            ?s ?p ?o .
        }
        WHERE {
            ?s ?p ?o .
            FILTER NOT EXISTS {
                ?s ?p2 ?o2 .
                FILTER(?p != ?p2 && ?o != ?o2)
            }
        }
        "#
    ];

    for update in updates {
        store.update(update);
    }
    println!("Data cleaned");
}

pub(crate) fn merge_list_items(store: &Store) {
    let query =
        r#"
    SELECT ?s1 ?s2 WHERE {
        ?s1 <http://schema.org/item> ?o.
        ?s2 <http://schema.org/item> ?o.
        FILTER(STR(?s1) < STR(?s2))
    }
    "#;

    let mut count = 0;

    if let QueryResults::Solutions(results) = store.query(query).unwrap() {
        for solution in results {
            let solution = solution.unwrap();
            let s1 = solution.get("s1").unwrap().to_string();
            let s2 = solution.get("s2").unwrap().to_string();

            // Transfer triples where s2 is subject
            let update_subject = format!(
                r#"
                DELETE {{ <{s2}> ?p ?o }}
                INSERT {{ <{s1}> ?p ?o }}
                WHERE  {{ <{s2}> ?p ?o }}
            "#
            );
            store.update(&update_subject);

            // Transfer triples where s2 is object
            let update_object = format!(
                r#"
                DELETE {{ ?sub ?pred <{s2}> }}
                INSERT {{ ?sub ?pred <{s1}> }}
                WHERE  {{ ?sub ?pred <{s2}> }}
            "#
            );
            store.update(&update_object);

            // Add owl:sameAs triple
            let insert_sameas = format!(
                r#"
                INSERT DATA {{ <{s2}> <http://www.w3.org/2002/07/owl#sameAs> <{s1}> }}
            "#
            );
            store.update(&insert_sameas);

            count += 1;
            if count % 100000 == 0 {
                println!("{count} merged...");
            }
        }
    }

    println!("Finished merging {count} duplicate entities.");
}

pub(crate) fn extract_literal(term: Option<&Term>) -> Option<String> {
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

pub fn skolemize(blank_node: String) -> String {
    return format!("<{}>", blank_node.replace("_", "urn:skolem"));
}

pub fn escape_html(data: String) -> String {
    data.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}

pub fn escape_js(data: String) -> String {
    data.replace("'", "\\'").replace("\"", "\\\"").replace("\n", "\\n").replace("\t", "\\t")
}

pub fn to_link(data: String) -> String {
    if data.starts_with("&lt;") && data.ends_with("&gt;") {
        let link = data.replace("&lt;", "/entity/<").replace("&gt;", ">").replace("#", "%23");
        format!("<a href=\"{}\">{}</a>", link, data)
    } else {
        data
    }
}

pub fn schema_link(data: String) -> String {
    if data.starts_with("&lt;http://schema.org/") && data.ends_with("&gt;") {
        let link = data.replace("&lt;", "/explore?id=<").replace("&gt;", ">");
        format!("<a href=\"{}\">{}</a>", link, data)
    } else {
        data
    }
}

pub fn linkify(data: &str) -> String {
    // let d = data.replace(")
    // println!("{data}");
    if data.starts_with("<http") {
        let d = data.replace("<", "").replace(">", "");
        return format!("<a href=\"{d}\">{}</a>", escape_html(data.to_string()));
    }
    escape_html(data.to_string())
}

pub fn verify_valid(uri: &String) -> bool {
    Url::parse(&uri).is_ok()
}

pub fn format_json(entity: String, props: Vec<QuerySolution>) -> String {
    let mut name = entity.clone();
    let mut image = String::new();
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
                    println!("yes");
                    is_img = true;
                }
            }
            (p, o) if
                image.is_empty() &&
                [
                    "<http://schema.org/image>",
                    "<http://schema.org/photo>",
                    "<http://schema.org/logo>",
                ].contains(&p)
            => {
                image = match o {
                    Literal(lit) => lit.value().to_string(),
                    NamedNode(named) => named.as_str().replace("<", "").replace(">", "<"),
                    _ => image,
                };
            }
            _ => {}
        }
        inside += &format!(
            "[\"{}\",  \'{}\'],",
            escape_js(
                escape_html(
                    p
                        .replace("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "type")
                        .replace("<http://schema.org/", ":")
                        .replace(">", "")
                )
            ),
            escape_js(escape_html(o.to_string()))
        );
    }

    if is_img {
        image = entity.replace("<", "").replace(">", "");
    }
    let imagerow = if image.is_empty() { image } else { format!("image: \"{}\",", image) };
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

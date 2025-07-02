use std::fs::File;
use std::io::{BufWriter, Write};
use oxigraph::model::{Term};
use oxigraph::model::Term::{NamedNode, Literal};
use regex::{Match, Regex};
use url::Url;
use std::io::{BufRead, BufReader};


pub(crate) fn preprocess(nquads_file: &str){
    let infile = File::open(nquads_file).unwrap();
    let outfile = File::create(format!("{}.nt", nquads_file)).unwrap();

    let mut reader = BufReader::new(infile);
    let mut writer = BufWriter::new(outfile);
    let mut line  = String::new();


    let re = Regex::new(r"<[^>]+\{(.*)\}[^>]*>").unwrap();
    let iri = Regex::new(r"<([^>]+)>").unwrap();
    let graph_name = Regex::new(r"<([^>]*)>\s*\.\n").unwrap();
    let bnode_regex = Regex::new(r"_:([A-Za-z0-9]+)").unwrap();


    while reader.read_line(&mut line).expect("Failed to read the file")!=0 {
        
        line = line.replace("\\uFFFD", "").replace("https://schema.org", "http://schema.org");

        //Book specific fix
        if line.contains("https://www.deutscherkunstverlag.de/") && line.contains("schema.org/url>"){
            let ms =  iri.find_iter(&line).collect::<Vec<Match>>();
            if ms[ms.len()-2].as_str()!=ms[ms.len()-1].as_str() {
                line = line.replace(ms[ms.len()-2].as_str(), ms[ms.len()-1].as_str());
            }
        }

        if line.contains("}") {
            match re.find(&line) {
                Some(m) => {
                    line = line.replace(m.as_str(), &m.as_str().replace("<", "\"").replace(">", "\""));

                },
                None => (),
            }
        }
        if line.contains("<@type:>"){
            line = line.replace("@type:", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        }

        line = graph_name.replace(&line, ".").to_string();
        line = bnode_regex.replace_all(&line, |caps: &regex::Captures| {
            skolemize(caps.get(0).unwrap().as_str().to_string())
        }).into_owned();

        let _ = writeln!(writer,"{}", line);
        line.clear();
    }
}

pub(crate) fn extract_literal(term: Option<&Term>)->Option<String>{
    match term {
        Some(t) => {
            match t {
                Literal(l)=> Some(l.value().to_string()),
                NamedNode(nnode) => Some(nnode.to_string().replace("<", "").replace(">", "")),
                _ => None
            }
        },
        None => None,
    }
}

pub fn skolemize(blank_node: String)->String{
    return format!("<{}>", blank_node.replace("_", "urn:skolem"));
}

pub fn escape_html(data: String)->String{
    data.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}

pub fn to_link(data: String) -> String{
    if data.starts_with("&lt;") && data.ends_with("&gt;") {
        let link = data.replace("&lt;", "/entity/<").replace("&gt;", ">");
        format!("<a href=\"{}\">{}</a>", link, data)
    }else {
    data
    }
}

pub fn schema_link(data: String)->String{
    if data.starts_with("&lt;http://schema.org/") && data.ends_with("&gt;") {
        let link = data.replace("&lt;", "/explore?id=<").replace("&gt;", ">");
        format!("<a href=\"{}\">{}</a>", link, data)
    }else {
    data
    }
}


pub fn linkify(data: &str)->String{
    // let d = data.replace(")
    // println!("{data}");
    if data.starts_with("<http") {
        let d = data.replace("<", "").replace(">", "");
      return format!("<a href=\"{d}\">{}</a>", escape_html(data.to_string()))
    }
    escape_html(data.to_string())
}

pub fn verify_valid(uri: &String) ->bool{
    Url::parse(&uri).is_ok()
}
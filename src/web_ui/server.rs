use core::option::Option::None;
use std::collections::{ HashMap, HashSet };
use std::fs::read_to_string;
use std::io::{ prelude::*, BufReader };
use std::net::{ TcpListener, TcpStream };
use std::path::Path;
use std::sync::Arc;
use std::thread;

use oxigraph::model::Term::Literal;

use crate::store::KG;
use crate::utils::{
    extract_query_param,
    escape_html,
    format_json,
    external_link,
    to_link,
    url_decode,
};
use crate::web_ui::html_templates::{
    explore_page,
    entity_page,
    history_page,
    index_page,
    query_page,
    routines_page,
};
use crate::store::StoreError;

enum Page {
    Index,
    Explore(String, u32),
    Query(Option<String>, Option<String>, Option<String>),
    Entity(String),
    Run(Vec<(bool, String, String)>),
    Scripts,
    Error,
    Redirect,
    History,
}

pub(crate) struct WebServer {
    dataset: Arc<KG>,
    port: u32,
}

impl WebServer {
    pub fn new(kg: KG, port: u32) -> WebServer {
        let kg = Arc::new(kg);
        WebServer { dataset: kg, port }
    }

    pub fn serve(&self) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port)).unwrap();
        println!("Listening on http://127.0.0.1:{}", self.port);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let dataset_clone = self.dataset.clone();
                    thread::spawn(move || {
                        let server = WebServer {
                            dataset: dataset_clone,
                            port: 0,
                        };
                        server.handle_connection(stream);
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    fn handle_connection(&self, mut stream: TcpStream) {
        let mut reader = BufReader::new(&mut stream);
        let mut request = String::new();

        loop {
            let mut line = String::new();
            let bytes_read = reader.read_line(&mut line).unwrap();
            if bytes_read == 0 || line == "\r\n" {
                break;
            }
            request.push_str(&line);
        }

        let first_line = request.lines().next().unwrap_or("");

        let full_path = first_line.split_whitespace().nth(1).unwrap_or("/");

        let (route, query_string) = match full_path.split_once('?') {
            Some((r, q)) => (r, Some(q)),
            _ => (full_path, None),
        };
        println!("{}", first_line);

        let (status_line, page) = match route {
            "/" => ("HTTP/1.1 200 OK", Page::Index),
            "/query" =>
                match query_string {
                    Some(qs) => {
                        (
                            "HTTP/1.1 200 OK",
                            Page::Query(
                                extract_query_param(qs, "query"),
                                extract_query_param(qs, "mode"),
                                extract_query_param(qs, "secondary")
                            ),
                        )
                    }
                    None => ("HTTP/1.1 200 OK", Page::Query(None, None, None)),
                }
            "/explore" =>
                match query_string {
                    Some(qs) =>
                        match extract_query_param(qs, "id") {
                            Some(id) => {
                                let page: u32 = match
                                    extract_query_param(qs, "page")
                                        .unwrap_or("1".to_string())
                                        .parse()
                                {
                                    Ok(num) => num,
                                    Err(_) => 1,
                                };
                                ("HTTP/1.1 200 OK", Page::Explore(id, page))
                            }
                            None => ("HTTP/1.1 400 BAD REQUEST", Page::Error),
                        }
                    None => ("HTTP/1.1 400 BAD REQUEST", Page::Error),
                }

            route if route.starts_with("/entity/") => {
                let fp = url_decode(full_path);
                let entity_name = &fp["/entity/".len()..];

                (
                    "HTTP/1.1 200 OK",
                    Page::Entity(entity_name.to_string().replace("%3C", "<").replace("%3E", ">")),
                )
            }
            "/routines" => {
                if let Some(qs) = query_string {
                    if qs.starts_with("entity") {
                        let ent = extract_query_param(qs, "entity").unwrap();
                        let mergeby_param = extract_query_param(qs, "mergeby").unwrap();
                        let mergeby: Vec<String> = mergeby_param
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .collect();
                        match self.dataset.merge_entities(ent, mergeby) {
                            Ok(()) => ("HTTP/1.1 200 OK", Page::Scripts),
                            Err(_) => ("HTTP/1.1 400 ERROR", Page::Error),
                        }
                    } else {
                        let queries = Self::parse_procedures(qs);

                        ("HTTP/1.1 200 OK", Page::Run(queries))
                    }
                } else {
                    ("HTTP/1.1 200 OK", Page::Scripts)
                }
            }
            "/dump" => {
                self.dataset.dump_store();
                ("HTTP/1.1 200 OK", Page::Redirect)
            }
            "/history" => { ("HTTP/1.1 200 OK", Page::History) }
            route if route.starts_with("/restore/") => {
                let v = route
                    .replace("/restore/version_", "")
                    .replace(".nt", "")
                    .parse::<u32>()
                    .unwrap();
                self.dataset.revert(v);
                ("HTTP/1.1 200 OK", Page::Redirect)
            }
            "/replay_history" => {
                let mut content_length = 0;

                for header in request.lines() {
                    if let Some(cl) = header.strip_prefix("Content-Length:") {
                        content_length = cl.trim().parse::<usize>().unwrap_or(0);
                    }
                }
                let mut body_buf = vec![0; content_length];
                reader.read_exact(&mut body_buf).unwrap();

                let payload = String::from_utf8(body_buf).unwrap();
                if let Err(_) = self.dataset.replay_history(payload) {
                    eprintln!("Error during replay_history");
                }
                ("HTTP/1.1 200 OK", Page::Redirect)
            }
            _ => ("HTTP/1.1 404 NOT FOUND", Page::Error),
        };

        let contents: String = match page {
            Page::Index => self.generate_index(),
            Page::Explore(id, page) => self.generate_explore(&id, page),
            Page::Query(Some(q), Some(mode), sq) => self.generate_query(&q, &mode, sq),
            Page::Query(None, _, _) => self.generate_query("", "query", None),
            Page::Query(Some(q), None, _) => self.generate_query(&q, "query", None),
            Page::Entity(uri) => self.generate_entity(&uri),
            Page::Scripts => self.generate_scripts(),
            Page::Run(scripts) => self.generate_run_results(scripts),
            Page::Error => "<html><body><h1>404 - Page Not Found</h1></body></html>".to_string(),
            Page::Redirect => include_str!("../../templates/redirect.html").to_string(),
            Page::History => self.generate_history(),
        };

        let response = format!(
            "{status_line}\r\nContent-Type: text/html; charset=UTF-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            contents.len(),
            contents
        );

        let _ = stream.write_all(response.as_bytes());
    }

    fn generate_index(&self) -> String {
        let q =
            r#"
SELECT ?t (COUNT(?s) AS ?count)
WHERE {
    ?s a ?t .
}
GROUP BY ?t
ORDER BY DESC(?count)
"#;

        let mut class_counts = vec![];

        let res = self.dataset.query(q);
        match res {
            Ok(result) => {
                for r in result {
                    let class = r.get("t").unwrap().to_string();
                    let cnt = match r.get("count").unwrap() {
                        Literal(literal) => literal.value().parse::<u32>().unwrap(),
                        _ => 0,
                    };
                    class_counts.push((class, cnt));
                }
            }
            Err(_) => panic!("SPARQL query failed"),
        }

        index_page(&self.dataset.get_name(), &class_counts)
    }

    fn generate_explore(&self, id: &str, page_num: u32) -> String {
        let objs = self.dataset.get_objects(
            &id.replace("%3C", "<").replace("%3E", ">"),
            50,
            (page_num - 1) * 50
        );
        let data = objs
            .iter()
            .map(|o| self.dataset.get_details(&o.to_string()).html_rep())
            .collect::<Vec<String>>()
            .join("");

        explore_page(id, page_num, &data)
    }

    fn generate_query(&self, q: &str, mode: &str, sq: Option<String>) -> String {
        let mut table_data = vec![];
        let mut headers = vec![];
        let mut message = "Query successfully executed".to_string();
        let mut message_type = "success";
        if !q.is_empty() {
            match mode {
                "query" => {
                    let query_result = self.dataset.query(q);
                    match query_result {
                        Ok(res) => {
                            if !res.is_empty() {
                                headers = res[0]
                                    .variables()
                                    .into_iter()
                                    .map(|var| var.clone().into_string())
                                    .collect::<Vec<String>>();

                                for r in res {
                                    let row = r
                                        .values()
                                        .into_iter()
                                        .map(|v| {
                                            match v {
                                                Some(t) => {
                                                    let val = t.to_string();
                                                    if val.starts_with('<') && val.ends_with('>') {
                                                        let inner = &val[1..val.len() - 1];
                                                        format!("<{}>", inner) // Keep brackets for now, interpreted in JS
                                                    } else {
                                                        val
                                                    }
                                                }
                                                None => "None".to_owned(),
                                            }
                                        })
                                        .collect::<Vec<String>>();
                                    table_data.push(row);
                                }
                            }
                        }
                        Err(StoreError::EvaluationError(ee)) => {
                            message = ee;
                            message_type = "danger";
                        }
                        Err(StoreError::UnsupportedError) => {
                            message = "The query is not yet supported".to_string();
                            message_type = "danger";
                        }
                    }
                }
                "update" => {
                    let query_result = self.dataset.update(q);
                    match query_result {
                        Ok(()) => {
                            self.dataset.write_to_history(format!("```sparql\n{}\n```", q));
                        }
                        Err(StoreError::EvaluationError(ee)) => {
                            message = ee;
                            message_type = "danger";
                        }
                        _ => (),
                    }
                }
                "advanced" => {
                    match self.dataset.iterative_update(&sq.clone().unwrap(), q) {
                        Ok(()) => {
                            self.dataset.write_to_history(
                                format!("```sparql\n{}\n#\n{}\n```", sq.unwrap(), q)
                            );
                        }
                        Err(StoreError::EvaluationError(ee)) => {
                            message = ee;
                            message_type = "danger";
                        }
                        _ => {
                            message = "Unknown error".to_string();
                            message_type = "danger";
                        }
                    }
                }

                _ => (),
            }
        }

        let result_rows = table_data.len();

        // JavaScript-safe string
        let mut table_rows_js_array = String::new();
        for row in table_data {
            let cells: Vec<String> = row
                .into_iter()
                .map(|cell| {
                    let escaped = cell.replace('\\', "\\\\").replace('"', "\\\"");
                    format!(r#""{}""#, escaped)
                })
                .collect();
            table_rows_js_array += &format!("[{}],", cells.join(","));
        }
        let table_headers_js_array = headers
            .iter()
            .map(|h| format!(r#""{}""#, h))
            .collect::<Vec<_>>()
            .join(",");
        let message_box = if q.is_empty() {
            ""
        } else {
            &format!(
                "<div class=\"alert alert-{}\" role=\"alert\"> {} </div>",
                message_type,
                message
            )
        };
        query_page(result_rows, &table_rows_js_array, &table_headers_js_array, message_box)
    }

    fn generate_entity(&self, entity: &str) -> String {
        let itm = self.dataset.get_details(entity);
        //Table 1 generation
        let table_1_query = format!(
            r#"
      SELECT ?pred ?obj WHERE {{
        {entity} ?pred ?obj .
      }}
      "#
        );
        let table_1_data = self.dataset.query(&table_1_query).unwrap_or(vec![]);
        let mut table_1 = String::new();
        for row in table_1_data {
            table_1 += &format!(
                "<tr>
          <td>{}</td>
          <td>{}</td>
        </tr>",
                escape_html(&row.get("pred").unwrap().to_string()),
                to_link(escape_html(&row.get("obj").unwrap().to_string()))
            );
        }
        //Table 2 generation
        let table_2_query = format!(
            r#"
      SELECT ?sub ?pred WHERE {{
        ?sub ?pred {entity} .
      }}
      "#
        );
        let table_2_data = self.dataset.query(&table_2_query).unwrap_or(vec![]);
        let mut table_2 = String::new();
        for row in table_2_data {
            table_2 += &format!(
                "<tr>
          <td>{}</td>
          <td>{}</td>
        </tr>",
                to_link(escape_html(&row.get("sub").unwrap().to_string())),
                escape_html(&row.get("pred").unwrap().to_string())
            );
        }

        let mut entity_types = String::new();
        for tp in itm.entity_types {
            entity_types += &format!(
                "<a href=\"/explore?id={}\">{}</a>",
                tp,
                escape_html(&tp.to_string())
            );
        }

        let name = &itm.name.unwrap_or("No name found".to_string());

        let img = if itm.images.is_empty() {
            String::new()
        } else {
            format!(
                "<img src=\"{}\" alt=\"{}\"  class=\"object-fit-cover d-block mx-auto\" style=\"
          height: 200px;
        \" />",
                itm.images.get(0).unwrap(),
                name
            )
        };

        let mut seen: HashSet<String> = HashSet::new();
        let mut items = vec![entity.to_string()];
        let mut connections = vec![];
        let mut cons = String::new();
        let mut jsons = String::new();

        while items.len() > 0 {
            let ent = items.get(0).unwrap().clone();
            if seen.contains(&ent) {
                items.remove(0);
                continue;
            }
            seen.insert(ent.clone());
            let simple_connections_query = format!(
                r#"PREFIX schema: <http://schema.org/>
                    SELECT DISTINCT ?predicate ?object
                    WHERE {{
                {ent} ?predicate ?object .
                    FILTER NOT EXISTS {{
                        ?object ?otherPredicate ?otherSubject .
                }}
                }}"#
            );
            let simple_connections = self.dataset
                .query(&simple_connections_query)
                .unwrap_or_default();
            let jsonrep = format_json(format!("{ent}"), simple_connections);
            jsons += &jsonrep;
            let complex_connections_query = format!(
                r#"PREFIX schema: <http://schema.org/>
                    SELECT DISTINCT ?predicate ?object
                    WHERE {{
                        {ent} ?predicate ?object .
                        ?object ?p ?oo .
                }}"#
            );

            let complex_connections = self.dataset
                .query(&complex_connections_query)
                .unwrap_or_default();
            for row in complex_connections {
                let cur = row.get("object").unwrap().to_string();
                connections.push((
                    ent.clone(),
                    cur.clone(),
                    row.get("predicate").unwrap().to_string(),
                ));
                items.push(cur);
            }
            items.remove(0);
        }
        for (s, t, l) in connections {
            cons += &format!("{{source: \"{}\", target: \"{}\", label: \"{}\"}},", s, t, l);
        }
        entity_page(
            &external_link(&entity),
            &name,
            &itm.description.unwrap_or("No description found".to_string()),
            &entity_types,
            &img,
            &table_1,
            &table_2,
            &jsons,
            &cons
        )
    }

    fn generate_scripts(&self) -> String {
        routines_page()
    }

    fn generate_run_results(&self, queries: Vec<(bool, String, String)>) -> String {
        let initial_count = self.dataset.count_lines();
        let mut err = false;
        let mut err_message = String::new();
        let mut err_index = 0usize;
        let mut success_scripts = vec![];
        let mut failed_script = None;

        for (i, (advanced, script_name, query)) in queries.iter().enumerate() {
            let result = if *advanced {
                let parts: Vec<&str> = query.split("#\n").collect();
                if parts.len() != 2 {
                    self.dataset.update(query)
                } else {
                    let (select_query, update_query) = (parts[0], parts[1]);
                    self.dataset.iterative_update(select_query, update_query)
                }
            } else {
                self.dataset.update(query)
            };
            match result {
                Ok(_) => {
                    success_scripts.push(script_name.clone());
                    self.dataset.write_to_history(script_name.to_string());
                }
                Err(StoreError::EvaluationError(ee)) => {
                    err_message = ee;
                    err = true;
                    err_index = i;
                    failed_script = Some(script_name.clone());
                    break;
                }
                Err(StoreError::UnsupportedError) => {
                    err = true;
                    err_index = i;
                    failed_script = Some(script_name.clone());
                    break;
                }
            }
        }

        let final_count = self.dataset.count_lines();
        let diff = (final_count as i64) - (initial_count as i64);
        let action = if diff >= 0 { "Inserted" } else { "Deleted" };
        let count = diff.abs();
        if err {
            let skipped_scripts = queries
                .iter()
                .skip(err_index + 1)
                .map(|(_, name, _)| format!("<li>{}</li>", name))
                .collect::<String>();

            let ran_scripts = success_scripts
                .iter()
                .map(|name| format!("<li>{}</li>", name))
                .collect::<String>();

            let failed_name = failed_script.unwrap_or_else(|| "Unknown".to_string());

            format!(
                r#"
    <!DOCTYPE html>
    <html lang="en" data-bs-theme="dark">
    <head>
      <meta charset="UTF-8">
      <title>Error</title>
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="d-flex justify-content-center align-items-center vh-100">
      <div class="text-center">
        <h1 class="text-danger mb-4">Something went wrong</h1>
        <div class="alert alert-danger text-start mx-auto" style="max-width: 500px;">
          <p><strong>Ran successfully:</strong></p>
          <ul>{ran_scripts}</ul>
          <p><strong>Failed on:</strong></p>
          <ul><li class="text-danger">{failed_name}</li></ul>
          <p class="alert alert-danger"> {err_message}</p>
          <p><strong>Skipped:</strong></p>
          <ul>{skipped_scripts}</ul>
           <p><strong>{action}:</strong> {count} triples</p>
        </div>
        <a href="/routines" class="btn btn-danger mt-3">Return</a>
      </div>
    </body>
    </html>
    "#
            )
        } else {
            let script_list = success_scripts
                .iter()
                .map(|name| format!("<li>{}</li>", name))
                .collect::<String>();

            format!(
                r#"
    <!DOCTYPE html>
    <html lang="en" data-bs-theme="dark">
    <head>
      <meta charset="UTF-8">
      <title>Success</title>
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="d-flex justify-content-center align-items-center vh-100">
      <div class="text-center">
        <h1 class="text-success mb-4">Success!</h1>
        <div class="alert alert-success text-start mx-auto" style="max-width: 500px;">
          <p><strong>{action}:</strong> {count} triples</p>
          <p><strong>Scripts executed:</strong></p>
          <ul>{script_list}</ul>
        </div>
        <a href="/routines" class="btn btn-success mt-3">Return</a>
      </div>
    </body>
    </html>
    "#
            )
        }
    }

    fn parse_procedures(query_string: &str) -> Vec<(bool, String, String)> {
        let query_string = url_decode(query_string);

        let mut selections: HashMap<String, Vec<String>> = HashMap::new();

        for pair in query_string.split('&') {
            if let Some((key, _value)) = pair.split_once('=') {
                if let Some((file, proc)) = key.split_once("::") {
                    let file = url_decode(file);
                    let proc = url_decode(proc);
                    if let Some(l) = selections.get_mut(&file) {
                        l.push(proc);
                    } else {
                        selections.insert(file, vec![proc]);
                    }
                }
            }
        }
        let mut found_queries = Vec::new();

        for key in selections.keys() {
            let wanted = selections.get(key).unwrap();
            let path = Path::new("routines").join(&key);
            if let Ok(content) = read_to_string(&path) {
                let mut current_name = String::new();
                let mut current_query = String::new();
                let mut in_proc = false;
                let mut is_advanced = false;

                for line in content.lines() {
                    if line.starts_with("##") {
                        if in_proc && wanted.contains(&current_name) {
                            found_queries.push((
                                is_advanced,
                                format!("{}::{}", key, current_name),
                                current_query.trim().to_string(),
                            ));
                        }
                        is_advanced = line.ends_with("@advanced");
                        current_name = line.trim_start_matches("##").trim().to_string();
                        current_query.clear();
                        in_proc = true;
                    } else if in_proc {
                        current_query.push_str(line);
                        current_query.push('\n');
                    }
                }

                if in_proc && wanted.contains(&current_name) {
                    found_queries.push((
                        is_advanced,
                        format!("{}::{}", key, current_name),
                        current_query.trim().to_string(),
                    ));
                }
            }
        }
        found_queries
    }

    fn generate_history(&self) -> String {
        let input = self.dataset.get_history();
        let lines = input.lines().map(str::trim);
        let mut inside = String::new();
        let mut sparql_block = String::new();
        let mut in_sparql = false;

        for line in lines {
            if line.starts_with("```sparql") {
                in_sparql = true;
                sparql_block.clear();
            } else if line.starts_with("```") && in_sparql {
                in_sparql = false;
                inside.push_str(
                    r#"<div class="card mb-3 shadow-sm">
          <div class="card-header bg-light text-dark">SPARQL Script</div>
          <div class="card-body">
            <pre class="bg-dark border p-3"><code>"#
                );
                inside.push_str(&escape_html(&sparql_block));
                inside.push_str("</code></pre>\n  </div>\n</div>\n");
            } else if in_sparql {
                sparql_block.push_str(line);
                sparql_block.push('\n');
            } else if let Some((file, desc)) = line.split_once("::") {
                inside.push_str(
                    &format!(
                        r#"<div class="card mb-3 shadow-sm">
          <div class="card-header bg-secondary text-white">{}</div>
          <div class="card-body">
            <span class="badge bg-info text-dark">Change</span> {}
          </div>
        </div>
        "#,
                        escape_html(&file.to_string()),
                        escape_html(&desc.to_string())
                    )
                );
            } else if line.starts_with("Dumping") {
                inside.push_str(
                    &format!(
                        r#"<div class="card mb-3 shadow-sm">
          <div class="card-header bg-success text-white">Dump created!</div>
          <div class="card-body">
            <span class="badge bg-info text-dark">Dump file</span> {}
          </div>
          <div class='d-flex justify-content-center py-2'>
          <a href="/restore/{}" class='btn btn-danger'>Revert to this version(all of the following changes and dumps will be lost)</a>
          </div>
        </div>"#,
                        line.replace("Dumping store to", ""),
                        line.replace(
                            &format!("Dumping store to ./data/{}/", self.dataset.get_name()),
                            ""
                        )
                    )
                );
            }
        }
        history_page(inside)
    }
}

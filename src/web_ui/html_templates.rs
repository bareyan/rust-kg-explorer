use std::{ env, fs };
use std::path::Path;

use crate::{ named_args, utils::escape_html, web_ui::templetization::Template };
use crate::web_ui::templetization::include_str;

const NAV: &str = include_str!("../../templates/parts/nav.html");

pub(crate) fn index_page(dataset_name: &str, class_counts: &[(String, u32)]) -> String {
    let mut all_cards = String::new();

    for (index, (class, count)) in class_counts.iter().enumerate() {
        all_cards += &format!(
            r#"<div class="col-md-4 mb-3 card-entry" data-index="{}" style="display: none;">{}</div>"#,
            index,
            class_card(class, *count)
        );
    }

    let total_cards = class_counts.len().to_string();

    let template = Template::new(
        include_str!("../../templates/index.html"),
        &["nav", "ds_name", "all_cards", "total_cards"]
    );

    template.render(
        named_args!(
            nav = NAV,
            ds_name = dataset_name,
            all_cards = &all_cards,
            total_cards = &total_cards
        )
    )
}

pub(crate) fn explore_page(data: &str, navigation: &str) -> String {
    let template = Template::new(
        include_str!("../../templates/explore.html"),
        &["nav", "navigation", "data"]
    );

    template.render(named_args!(nav = NAV, navigation = navigation, data = data))
}

pub(crate) fn query_page(
    nb_results: usize,
    table_rows_js_array: &str,
    table_headers_js_array: &str,
    message: &str
) -> String {
    let htmlcode = &include_str("templates/query.html");
    let html_template = Template::new(htmlcode, &["nav", "message", "nb_results", "js"]);

    let jscode = &include_str("templates/query.js");
    let js_template = Template::new(
        jscode,
        &["table_rows_js_array", "table_headers_js_array", "api_key"]
    );

    let api_key = env::var("API_KEY").unwrap_or("YOUR GOOGLE AI API KEY".to_string());

    let js = &js_template.render(
        named_args!(
            table_headers_js_array = table_headers_js_array,
            table_rows_js_array = table_rows_js_array,
            api_key = &api_key
        )
    );
    let nb_results = &nb_results.to_string();

    html_template.render(
        named_args!(nav = NAV, message = message, nb_results = nb_results, js = js)
    )
}

pub(crate) fn entity_page(
    uri: &str,
    name: &str,
    description: &str,
    otype: &str,
    image: &str,
    table_1: &str,
    table_2: &str,
    jsons: &str,
    cons: &str
) -> String {
    let js = &include_str("templates/graph_renderer.js");
    let html = &include_str("templates/entity.html");
    let template = Template::new(
        html,
        &[
            "nav",
            "image",
            "uri",
            "otype",
            "name",
            "description",
            "table_1",
            "table_2",
            "js",
            "nodes",
            "cons",
        ]
    );

    template.render(
        named_args!(
            nav = NAV,
            image = image,
            uri = uri,
            otype = otype,
            name = name,
            description = description,
            table_1 = table_1,
            table_2 = table_2,
            js = js,
            nodes = jsons,
            cons = cons
        )
    )
}

pub(crate) fn object_card(name: &str, description: &str, image: &str, id: &str) -> String {
    let template = Template::new(
        include_str!("../../templates/parts/object_card.html"),
        &["nav", "id", "image", "name", "description"]
    );

    template.render(
        named_args!(nav = NAV, id = id, image = image, name = name, description = description)
    )
}

pub(crate) fn class_card(name: &str, count: u32) -> String {
    let entity_name = &name.split("/").last().unwrap_or_default().replace(">", "");
    let count = &format!("{count}");
    let template = Template::new(
        include_str!("../../templates/parts/class_card.html"),
        &["nav", "name", "entity_name", "count"]
    );

    template.render(named_args!(nav = NAV, name = name, entity_name = entity_name, count = count))
}

pub(crate) fn routines_page() -> String {
    let mut script_cards = String::new();

    if let Ok(entries) = fs::read_dir("routines") {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sparql") {
                if let Ok(content) = fs::read_to_string(&path) {
                    script_cards += &script_card(&path, &content);
                }
            }
        }
    }
    let template = Template::new(
        include_str!("../../templates/routines.html"),
        &["nav", "script_cards"]
    );

    template.render(named_args!(nav = NAV, script_cards = &script_cards))
}

pub(crate) fn history_page(inside: String) -> String {
    let template = Template::new(include_str!("../../templates/history.html"), &["nav", "inside"]);

    template.render(named_args!(nav = NAV, inside = inside))
}

fn script_card(path: &Path, content: &str) -> String {
    let file_name = path.file_name().unwrap().to_string_lossy();
    let mut lines = content.lines();
    let description = lines.next().unwrap_or("").trim_start_matches("###").trim();

    let mut body = String::new();
    let mut current_proc_name = String::new();
    let mut current_proc_query = String::new();
    let mut in_proc = false;

    for line in lines {
        if line.starts_with("##") {
            if in_proc {
                body += &procedure_section(&file_name, &current_proc_name, &current_proc_query);
                current_proc_query.clear();
            }
            current_proc_name = line.trim_start_matches("##").trim().to_string();
            in_proc = true;
        } else if in_proc {
            current_proc_query.push_str(line);
            current_proc_query.push('\n');
        }
    }

    if in_proc {
        body += &procedure_section(&file_name, &current_proc_name, &current_proc_query);
    }

    format!(
        r#"<div class="card mb-4">
    <div class="card-header d-flex justify-content-between align-items-center">
        <div>
            <h5 class="mb-0">{}</h5>
            <small class="text-muted">{}</small>
        </div>
        <div>
            <input type="checkbox" class="form-check-input" onchange="toggleFile(this)" data-file="{}">
            <label class="form-check-label ms-1">Run entire file</label>
        </div>
    </div>
    <div class="card-body">{}</div>
</div>"#,
        file_name,
        description,
        file_name,
        body
    )
}

fn procedure_section(file: &str, name: &str, query: &str) -> String {
    let query = escape_html(&query.to_string());
    let elem_id = format!("{file}::{name}");

    format!(
        r#"<div class="mb-3">
    <div class="form-check">
        <input class="form-check-input file-proc" type="checkbox"
               name="{elem_id}"
               data-file="{file}" data-id="{elem_id}"
               onchange="toggleProcedure(this)">
        <label class="form-check-label fw-bold">{name}</label>
    </div>
    <pre class="bg-body border rounded p-2 mt-2" style="display:none" id="{elem_id}"><code>{query}</code></pre>
</div>"#
    )
}

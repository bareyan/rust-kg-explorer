use std::env;

use crate::{ named_args, web_ui::templetization::{ Template } };
use crate::web_ui::templetization::include_str;

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
        &["ds_name", "all_cards", "total_cards"]
    );

    template.render(
        named_args!(ds_name = dataset_name, all_cards = &all_cards, total_cards = &total_cards)
    )
}

pub(crate) fn explore_page(data: &str, navigation: &str) -> String {
    let template = Template::new(
        include_str!("../../templates/explore.html"),
        &["navigation", "data"]
    );

    template.render(named_args!(navigation = navigation, data = data))
}

pub(crate) fn query_page(
    nb_results: usize,
    table_rows_js_array: &str,
    table_headers_js_array: &str,
    message: &str
) -> String {
    // let htmlcode= &include_str("templates/query.html");
    let html_template = Template::new(
        include_str!("../../templates/query.html"),
        &["message", "nb_results", "js"]
    );

    // let jscode = &include_str("templates/query.js");
    let js_template = Template::new(
        include_str!("../../templates/query.js"),
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

    html_template.render(named_args!(message = message, nb_results = nb_results, js = js))
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
        &["id", "image", "name", "description"]
    );

    template.render(named_args!(id = id, image = image, name = name, description = description))
}

pub(crate) fn class_card(name: &str, count: u32) -> String {
    let entity_name = &name.split("/").last().unwrap_or_default().replace(">", "");
    let count = &format!("{count}");
    let template = Template::new(
        include_str!("../../templates/parts/class_card.html"),
        &["name", "entity_name", "count"]
    );

    template.render(named_args!(name = name, entity_name = entity_name, count = count))
}

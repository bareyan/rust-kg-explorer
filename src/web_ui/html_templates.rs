use crate::{named_args, web_ui::templetization::Template};


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


  let template = Template::new(include_str!("../../templates/index.html"), &["ds_name", "all_cards", "total_cards"]);

  template.render(named_args!(
    ds_name = dataset_name,
    all_cards = &all_cards,
    total_cards = &total_cards
  ))
}

pub(crate) fn explore_page(data:&str, navigation:&str)->String{

    let template = Template::new(include_str!("../../templates/explore.html"), &["navigation", "data"]);

    template.render(named_args!(
      navigation = navigation,
      data = data
    ))
  
}

pub(crate) fn query_page(nb_results: usize, table_rows_js_array: &str, table_headers_js_array: &str, message: &str) -> String {
  let html_template = Template::new(include_str!("../../templates/query.html"), &["message", "nb_results", "js"]);

  let js_template = Template::new(include_str!("../../templates/query.js"),  &["table_rows_js_array", "table_headers_js_array"]);

  let js = &js_template.render(named_args!(
    table_headers_js_array = table_headers_js_array,
    table_rows_js_array = table_rows_js_array
  ));

  let nb_results  = &nb_results.to_string();

  html_template.render(named_args!(
    message=message,
    nb_results = nb_results,
    js = js
  ))

}

pub(crate) fn entity_page(uri:&str, name:&str, description:&str, otype:&str, image:&str, table_1:&str, table_2: &str) ->String{
    let template = Template::new(include_str!("../../templates/entity.html"), &["image", "uri", "otype", "name", "description", "table_1", "table_2"]);

    template.render(named_args!(
      image=image,
      uri = uri,
      otype = otype,
      name=name,
      description=description,
      table_1=table_1,
      table_2 = table_2
    ))
  }

pub(crate) fn object_card(name:&str, description:&str, image: &str, id:&str)->String{

  let template = Template::new(include_str!("../../templates/parts/object_card.html"), &["id", "image", "name",  "description"]);

    template.render(named_args!(
      id=id,
      image=image,
      name=name,
      description=description
    ))
}

pub(crate) fn class_card(name:&str, count: u32)->String{
  let entity_name = &name.split("/").last().unwrap_or_default().replace(">", "");
  let count = &format!("{count}");
  let template = Template::new(include_str!("../../templates/parts/class_card.html"), &["name", "entity_name", "count"]);

  template.render(named_args!(
    name = name,
    entity_name = entity_name,
    count = count
  ))
}
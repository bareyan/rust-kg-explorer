use crate::web_ui::html_templates::object_card;
use oxigraph::model::Term;

pub struct Item {
    node: Term,
    pub entity_types: Vec<Term>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub images: Vec<String>,
}
impl Item {
    pub fn new(
        node: Term,
        types: Vec<Term>,
        name: Option<String>,
        description: Option<String>,
        imgs: Vec<String>
    ) -> Item {
        Item {
            node,
            entity_types: types,
            name,
            description,
            images: imgs,
        }
    }
    // pub fn empty(node: Term) -> Item{
    //     Item {
    //         node: node,
    //         entity_types: vec![],
    //         name: None,
    //         description: None,
    //         images: vec![]
    //     }
    // }
    pub fn _print(&self) {
        println!("{}", self);
    }

    pub fn html_rep(&self) -> String {
        let image = self.images.first().map(String::as_str).unwrap_or("").to_string();
        let name = self.name.as_deref().unwrap_or("Unknown").to_string();
        let description = self.description
            .as_deref()
            .unwrap_or("No description available")
            .to_string();
        // no unnecessary clones

        let id = match &self.node {
            Term::NamedNode(named_node) => named_node.as_str(),
            Term::BlankNode(blank_node) => blank_node.as_str(),
            Term::Literal(_) => panic!("A literal cannot be an object"),
            Term::Triple(_) => panic!("Wrong SPARQL request. Tripple as result is not expected"),
        };
        object_card(&name, &description, &image, &id.replace("#", "%23"))
    }
}
impl From<&Item> for String {
    fn from(value: &Item) -> Self {
        format!(
            "-------------------------\nA Node {},\nhas name: {},\n{}\n-------------------------------",
            value.node,
            value.name.clone().unwrap_or("No name available".to_string()),
            value.description.clone().unwrap_or("No description available".to_string())
        )
    }
}

impl std::fmt::Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mess: String = self.into();
        write!(f, "{}", mess)
    }
}

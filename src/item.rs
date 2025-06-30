use oxigraph::model::{Term};
use crate::web_ui::html_templates::object_card;

pub struct Item{
    node: Term,
    pub entity_types: Vec<Term>,
    pub name: Option<String>,
    pub description: Option<String>, 
    pub images: Vec<String>
}
impl Item{
    pub fn new(node: Term, types: Vec<Term>, name: Option<String>, description: Option<String>, imgs: Vec<String>) -> Item{
        Item {
            node: node,
            entity_types: types,
            name: name, 
            description: description, 
            images: imgs
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
    pub fn _print(&self){
        println!("-------------------------\nA Node {},\nhas name: {},\n{}\n-------------------------------", self.node, self.name.clone().unwrap_or("No name available".to_string()), self.description.clone().unwrap_or("No description available".to_string()));
    }

    pub fn html_rep(&self) -> String {
        let image = self.images.get(0).unwrap_or(&"".to_string()).clone();
        let name = self.name.clone().unwrap_or( "Unknown".to_string());
        let description = self.description.clone().unwrap_or( "No description available".to_string());
        let id = match &self.node {
            Term::NamedNode(named_node) => named_node.as_str(),
            Term::BlankNode(blank_node) => blank_node.as_str(),
            Term::Literal(_) => panic!("A literal cannot be an object"),
            Term::Triple(_) => panic!("Wrong SPARQL request. Tripple as result is not expected")   ,
        };
        object_card(&name, &description, &image, &id)
    }
    
}
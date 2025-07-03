use std::{collections::HashMap, fs};

pub struct Template<'a> {
    text: &'a str,
    expected_keys: Vec<&'a str>,
}

impl<'a> Template<'a> {
    pub fn new(text: &'a str, expected_keys: &[&'a str]) -> Self {
        Self {
            text,
            expected_keys: expected_keys.to_vec(),
        }
    }

    pub fn render(&self, args: HashMap<&'a str, &'a str>) -> String {
        for &key in &self.expected_keys {
            if !args.contains_key(key) {
                panic!("Missing value for placeholder: {}", key);
            }
        }

        let mut result = self.text.to_owned();
        for &key in &self.expected_keys {
            let placeholder = format!("[[{}]]", key);
            if let Some(value) = args.get(key) {
                result = result.replace(&placeholder, value);
            }
        }
        result
    }
}
#[macro_export]
macro_rules! named_args {
    ($($key:ident = $value:expr),* $(,)?) => {{
        let mut map = std::collections::HashMap::new();
        $(
            map.insert(stringify!($key), $value);
        )*
        map
    }};
}



pub fn include_str(path: &str)->String{
    fs::read_to_string(path).unwrap()
}
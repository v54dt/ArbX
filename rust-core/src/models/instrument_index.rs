use std::collections::HashMap;

use super::market::OrderBook;

pub struct InstrumentIndex {
    key_to_id: HashMap<String, usize>,
    id_to_key: Vec<String>,
}

impl Default for InstrumentIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InstrumentIndex {
    pub fn new() -> Self {
        Self {
            key_to_id: HashMap::new(),
            id_to_key: Vec::new(),
        }
    }

    pub fn register(&mut self, key: &str) -> usize {
        if let Some(&id) = self.key_to_id.get(key) {
            return id;
        }
        let id = self.id_to_key.len();
        self.key_to_id.insert(key.to_string(), id);
        self.id_to_key.push(key.to_string());
        id
    }

    pub fn get_id(&self, key: &str) -> Option<usize> {
        self.key_to_id.get(key).copied()
    }

    pub fn len(&self) -> usize {
        self.id_to_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_key.is_empty()
    }
}

pub struct BookStore {
    books: Vec<Option<OrderBook>>,
}

impl BookStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            books: vec![None; capacity],
        }
    }

    pub fn insert(&mut self, id: usize, book: OrderBook) {
        if id < self.books.len() {
            self.books[id] = Some(book);
        }
    }

    pub fn get(&self, id: usize) -> Option<&OrderBook> {
        self.books.get(id).and_then(|b| b.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_returns_sequential_ids() {
        let mut idx = InstrumentIndex::new();
        assert_eq!(idx.register("a"), 0);
        assert_eq!(idx.register("b"), 1);
        assert_eq!(idx.register("c"), 2);
    }

    #[test]
    fn register_is_idempotent() {
        let mut idx = InstrumentIndex::new();
        let id = idx.register("a");
        assert_eq!(idx.register("a"), id);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn get_id_returns_none_for_unknown() {
        let idx = InstrumentIndex::new();
        assert!(idx.get_id("missing").is_none());
    }

    #[test]
    fn len_and_is_empty() {
        let mut idx = InstrumentIndex::new();
        assert!(idx.is_empty());
        idx.register("x");
        assert_eq!(idx.len(), 1);
        assert!(!idx.is_empty());
    }
}

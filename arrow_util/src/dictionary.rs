//! Contains a structure to map from strings to u32 symbols based on
//! string interning.
use hashbrown::HashMap;

use crate::string::PackedStringArray;
use num_traits::{AsPrimitive, FromPrimitive, Zero};

/// A String dictionary that builds on top of `PackedStringArray` adding O(1)
/// index lookups for a given string
///
/// Heavily inspired by the string-interner crate
#[derive(Debug)]
pub struct StringDictionary<K> {
    hash: ahash::RandomState,
    /// Used to provide a lookup from string value to DID
    ///
    /// Note: K's hash implementation is not used, instead the raw entry
    /// API is used to store keys w.r.t the hash of the strings themselves
    ///
    dedup: HashMap<K, (), ()>,
    /// Used to store strings
    storage: PackedStringArray<K>,
}

impl<K: AsPrimitive<usize> + FromPrimitive + Zero> Default for StringDictionary<K> {
    fn default() -> Self {
        Self {
            hash: ahash::RandomState::new(),
            dedup: Default::default(),
            storage: PackedStringArray::new(),
        }
    }
}

impl<K: AsPrimitive<usize> + FromPrimitive + Zero> StringDictionary<K> {
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns the id corresponding to value, adding an entry for the
    /// id if it is not yet present in the dictionary.
    pub fn lookup_value_or_insert(&mut self, value: &str) -> K {
        use hashbrown::hash_map::RawEntryMut;

        let hasher = &self.hash;
        let storage = &mut self.storage;
        let hash = hash_str(hasher, value);

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |key| value == storage.get(key.as_()).unwrap());

        match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let index = storage.append(value);
                let key =
                    K::from_usize(index).expect("failed to fit string index into dictionary key");
                *entry
                    .insert_with_hasher(hash, key, (), |key| {
                        let string = storage.get(key.as_()).unwrap();
                        hash_str(hasher, string)
                    })
                    .0
            }
        }
    }

    /// Returns the ID in self.dictionary that corresponds to `value`,
    /// if any. No error is returned to avoid an allocation when no value is
    /// present
    pub fn id(&self, value: &str) -> Option<K> {
        let hash = hash_str(&self.hash, value);
        self.dedup
            .raw_entry()
            .from_hash(hash, |key| value == self.storage.get(key.as_()).unwrap())
            .map(|(&symbol, &())| symbol)
    }

    /// Returns the ID in self.dictionary that corresponds to `value`, if any.
    /// Returns an error if no such value is found. Does not add the value
    /// to the dictionary.
    pub fn lookup_value(&self, value: &str) -> Option<K> {
        self.id(value)
    }

    /// Returns the str in self.dictionary that corresponds to `id`
    pub fn lookup_id(&self, id: K) -> Option<&str> {
        self.storage.get(id.as_())
    }

    pub fn size(&self) -> usize {
        self.storage.size() + self.dedup.len() * std::mem::size_of::<K>()
    }

    pub fn values(&self) -> &PackedStringArray<K> {
        &self.storage
    }
}

fn hash_str(hasher: &ahash::RandomState, value: &str) -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut state = hasher.build_hasher();
    value.hash(&mut state);
    state.finish()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dictionary() {
        let mut dictionary = StringDictionary::<i32>::new();

        let id1 = dictionary.lookup_value_or_insert("cupcake");
        let id2 = dictionary.lookup_value_or_insert("cupcake");
        let id3 = dictionary.lookup_value_or_insert("womble");

        let id4 = dictionary.lookup_value("cupcake").unwrap();
        let id5 = dictionary.lookup_value("womble").unwrap();

        let cupcake = dictionary.lookup_id(id4).unwrap();
        let womble = dictionary.lookup_id(id5).unwrap();

        let arrow_expected = StringArray::from(vec!["cupcake", "womble"]);
        let arrow_actual = dictionary.values().to_arrow();

        assert_eq!(id1, id2);
        assert_eq!(id1, id4);
        assert_ne!(id1, id3);
        assert_eq!(id3, id5);

        assert_eq!(cupcake, "cupcake");
        assert_eq!(womble, "womble");

        assert!(dictionary.id("foo").is_none());
        assert!(dictionary.lookup_id(-1).is_none());
        assert_eq!(arrow_expected, arrow_actual);
    }
}

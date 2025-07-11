use bit_vec::BitVec;
use fnv::{FnvHasher};
use std::hash::{Hasher};
use std::collections::hash_map::DefaultHasher;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    // Store bits as Vec<u8> for serialization 
    // We could delete Serialize and Deserialize instead of defining custom functions for those
    #[serde(serialize_with = "serialize_bitvec", deserialize_with = "deserialize_bitvec")] 
    bits: BitVec,
    hash_functions: usize,
    expected_items: usize,
}

// Custom serialization functions
fn serialize_bitvec<S>(bits: &BitVec, serializer: S) -> Result<S::Ok, S::Error>
where 
    S: serde::Serializer,
{
    let bytes = bits.to_bytes();
    bytes.serialize(serializer)   
}

fn deserialize_bitvec<'de, D>(deserializer: D) -> Result<BitVec, D::Error>
where 
    D: serde::Deserializer<'de>,
{
    let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
    Ok(BitVec::from_bytes(&bytes))
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size of the bit vector
        let bit_size = (-(expected_items as f64 * false_positive_rate.ln()) / (2.0_f64.ln().powi(2))).ceil() as usize;

        // Calculate optimal number of hash functions
        let hash_functions = ((bit_size as f64 / expected_items as f64) * 2.0_f64.ln()).ceil() as usize;

        Self {
            bits: BitVec::from_elem(bit_size, false),
            hash_functions,
            expected_items,
        }
    }

    pub fn with_size(bit_size: usize, hash_functions: usize) -> Self {
        Self {
            bits: BitVec::from_elem(bit_size, false),
            hash_functions,
            expected_items: 0, // Not used in this constructor
        }
    }

    pub fn insert(&mut self, item: &str) {
        let positions = self.get_hash_positions(item);
        for position in positions {
            self.bits.set(position, true);
        }
    }

    pub fn contains(&self, item: &str) -> bool {
        let positions = self.get_hash_positions(item);
        for position in positions {
            if !self.bits.get(position).unwrap_or(false) {
                return false;
            }
        }
        true
    }

    pub fn clear(&mut self) {
        self.bits.clear();
        self.bits.grow(self.bits.capacity(), false);
    }

    pub fn len(&self) -> usize {
        self.bits.len()
    }

    pub fn estimated_false_positive_rate(&self) -> f64 {
        let set_bits = self.bits.iter().filter(|&b| b).count();
        let total_bits = self.bits.len();

        if total_bits == 0 {
            return 0.0;
        }

        let ratio = set_bits as f64 / total_bits as f64;
        ratio.powf(self.hash_functions as f64)
    }

    fn hash_item(&self, item: &str, seed: u64) -> u64 {
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = FnvHasher::default();

        hasher1.write(item.as_bytes());
        hasher1.write_u64(seed);

        hasher2.write(item.as_bytes());
        hasher2.write_u64(seed.wrapping_mul(17));

        hasher1.finish().wrapping_add(hasher2.finish().wrapping_mul(seed))
    }

    fn get_hash_positions(&self, item: &str) -> Vec<usize> {
        let mut positions = Vec::with_capacity(self.hash_functions);

        let hash1 = self.hash_item(item, 0);
        let hash2 = self.hash_item(item, 1);

        for i in 0..self.hash_functions {
            let hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            let position = (hash % self.bits.len() as u64) as usize;
            positions.push(position);
        }

        positions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        
        // Insert some items
        bloom.insert("key1");
        bloom.insert("key2");
        bloom.insert("key3");
        
        // Test containment
        assert!(bloom.contains("key1"));
        assert!(bloom.contains("key2"));
        assert!(bloom.contains("key3"));
        
        println!("Bloom filter size: {} bits", bloom.len());
        println!("False positive rate: {:.4}", bloom.estimated_false_positive_rate());
    }

    #[test]
    fn test_bloom_filter_false_negatives() {
        let mut bloom = BloomFilter::new(10, 0.1);
        
        // Insert items
        for i in 0..5 {
            bloom.insert(&format!("key{}", i));
        }
        
        // Should never have false negatives
        for i in 0..5 {
            assert!(bloom.contains(&format!("key{}", i)), "False negative for key{}", i);
        }
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut bloom = BloomFilter::new(100, 0.01);
        
        bloom.insert("test");
        assert!(bloom.contains("test"));
        
        bloom.clear();
        // After clear, should not contain anything
        assert!(!bloom.contains("test"));
    }

    #[test]
    fn test_bloom_filter_custom_size() {
        let mut bloom = BloomFilter::with_size(1000, 3);
        
        bloom.insert("custom");
        assert!(bloom.contains("custom"));
        assert_eq!(bloom.len(), 1000);
    }
}
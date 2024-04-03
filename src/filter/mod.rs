use std::{cmp, f64::consts::LN_2, mem};

pub struct BloomFilter {
   filter: Vec<u8>,
   hash_count: u64,
   item_size: usize,
   filter_size: usize
}


impl BloomFilter {
    // create new bloom filter
    pub fn create( item_size: usize, fposivity_ratio: f64) -> Self {
      let bit_array_size = calculate_size_of_bitmap(item_size, fposivity_ratio);
      let num_of_hash = calculate_number_of_hash(bit_array_size, item_size) as u64;
      let filter = vec![0; bit_array_size];
      let filter_size = filter.len();
      Self {
         filter,
         hash_count: num_of_hash,
         item_size,
         filter_size
      }
    }

    // add item to bloom filter array
    pub fn add(&mut self, item: &str) {
      for _ in 0..self.hash_count {
         let hash = fastmurmur3::hash(item.as_bytes());
         let filter_index = (hash % 10) as usize;
         println!("{}", filter_index);
         self.filter[filter_index] = 1;
      }
    }

    //lookup the bloom filter for item existence 
    pub fn lookup(&self, item: &str) -> bool {
      for _ in 0..self.hash_count {
         let hash = fastmurmur3::hash(item.as_bytes());
         let filter_index = (hash % 10) as usize;

         if self.filter[filter_index] == 0 {
            return false
         }
      }
      return true
    }

    pub fn get_filter(&self) -> &Vec<u8> {
      &self.filter
    }

    pub fn get_filter_len(&self) -> usize {
      print!("{}", mem::size_of_val(self.get_filter()));
      self.filter.len()
    }
}

fn calculate_size_of_bitmap(item_size: usize, fposivity_ratio: f64) -> usize {
   let log2 = LN_2;
   let log2_2 = log2 * log2;

   let size = ((item_size as f64) * f64::ln(fposivity_ratio) / (-8.0 * log2_2)).ceil() as usize;
   size
}

fn calculate_number_of_hash(m: usize, n: usize) -> usize {
   let value = ((m as f64 / n as f64) * f64::ln(2.0f64));
   
   cmp::max( value as usize, 1)
}

// ??
fn calculate_item_location(item: &str) {}
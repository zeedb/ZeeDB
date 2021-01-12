use rand::Rng;

pub const DEFAULT_M: usize = 8;
pub const DEFAULT_K: usize = 200;
pub const MIN_K: usize = DEFAULT_M;
pub const MAX_K: usize = (1 << 16) - 1;

/// Histogram implements the approximate histogram described in:
///   "Streaming Quantiles Algorithms with Small Space and Update Time" https://arxiv.org/abs/1907.00236
/// It is based on the C++ implementation in https://github.com/apache/datasketches-cpp/tree/master/kll
#[derive(Debug)]
pub struct Histogram<T: Ord + Default> {
    /// k is the size of the final, largest level.
    k: usize,
    /// m is the minimum size of a level.
    m: usize,
    /// n is the number of items observed by this histogram.
    n: usize,
    /// levels[i] is the total number of items in levels i..end-1
    /// levels[end] is always 0.
    levels: Vec<usize>,
    /// items stores all levels, highest-to-lowest.
    /// items[levels[i + 1]...levels[i]] holds level i.
    items: Vec<T>,
    /// Other levels are always sorted.
    is_level_zero_sorted: bool,
}

impl<T: Ord + Default> Histogram<T> {
    pub fn with_capacity(k: usize) -> Self {
        Self::new(k, DEFAULT_M)
    }

    fn new(k: usize, m: usize) -> Self {
        assert!(MIN_K <= k && k <= MAX_K);

        Self {
            k,
            m,
            n: 0,
            levels: vec![0, 0],
            items: Vec::with_capacity(k),
            is_level_zero_sorted: false,
        }
    }

    pub fn insert(&mut self, value: T) {
        if self.is_full() {
            self.compress_while_updating();
        }
        self.items.push(value);
        self.levels[0] += 1;
        self.is_level_zero_sorted = false;
        self.n += 1;
    }

    pub fn merge(&mut self, other: Self) {
        // Replace self with an empty histogram, and rename self/other to left/right.
        let mut left = std::mem::replace(self, Self::new(self.k, self.m));
        let mut right = other;
        // Make sure left and right have the same number of levels.
        while left.levels.len() < right.levels.len() {
            left.levels.push(0);
        }
        while right.levels.len() < left.levels.len() {
            right.levels.push(0);
        }
        // Make sure left and right are fully sorted.
        if !left.is_level_zero_sorted {
            left.sort_level_zero();
        }
        if !right.is_level_zero_sorted {
            right.sort_level_zero();
        }
        // Combine statistics from left and right.
        self.n = left.n + right.n;
        self.is_level_zero_sorted = false;
        self.levels = (0..left.levels.len())
            .map(|i| left.levels[i] + right.levels[i])
            .collect();
        // Combine items from left and right.
        for level in (0..left.levels.len() - 1).rev() {
            // Copy items from left.
            let left_start = left.levels[level + 1];
            let left_end = left.levels[level];
            for i in left_start..left_end {
                self.items.push(std::mem::take(&mut left.items[i]));
            }
            // Copy items from right.
            let right_start = right.levels[level + 1];
            let right_end = right.levels[level];
            for i in right_start..right_end {
                self.items.push(std::mem::take(&mut right.items[i]));
            }
            // Sort the combined range.
            let start = left_start + right_start;
            let end = left_end + right_end;
            self.items[start..end].sort();
        }
        // Shrink back down to size.
        let target = total_capacity(self.k, self.m, self.levels.len() - 1);
        while self.items.len() > target {
            let level = self.find_level_to_compact();
            self.compact_level(level);
        }
        self.items.shrink_to(target);
    }

    pub fn quantiles(&mut self, fractions: &[f64]) -> Option<Vec<&T>> {
        // If no items have been added, quantile is unknown.
        if self.is_empty() {
            return None;
        }
        // Ensure that all levels are sorted.
        self.sort_level_zero();
        // Compute [(&value, weight), ..]
        let mut cdf = Vec::with_capacity(self.items.len());
        let mut total = 0.0;
        for level in 0..self.levels.len() - 1 {
            let start = self.levels[level + 1];
            let end = self.levels[level];
            let weight = (level as f64).exp2();
            for i in start..end {
                cdf.push((&self.items[i], weight));
                total += weight;
            }
        }
        // Convert [(&value, weight), ..] into cumulative distribution function [(&value, fraction)]
        cdf.sort_by_key(|(value, _)| *value);
        let mut sum = 0.0;
        for (_, weight) in &mut cdf {
            sum += *weight;
            *weight = sum / total;
        }
        // Find the median for each requested fraction.
        let mut values = Vec::with_capacity(fractions.len());
        for fraction in fractions {
            // The median is defined as a range [lower, upper)
            // lower is the largest value such that:
            //    p(data <= lower) < fraction
            // upper is the smallest value such that:
            //    fraction < p(data < upper)
            // Ideally, we would take the midpoint of these two points, but this isn't always defined.
            // Instead, we will return upper.
            let (value, sum) =
                match cdf.binary_search_by(|(_, probe)| probe.partial_cmp(fraction).unwrap()) {
                    Err(0) => cdf[0],
                    Err(i) if i == cdf.len() => cdf[i - 1],
                    Ok(i) | Err(i) if i == cdf.len() - 1 => cdf[i],
                    Ok(i) | Err(i) => cdf[i + 1],
                };
            values.push(value);
        }

        Some(values)
    }

    pub fn rank(&self, value: T) -> Option<f64> {
        // If no items have been added, rank is unknown.
        if self.is_empty() {
            return None;
        }
        let mut level = 0;
        let mut weight = 1;
        let mut total = 0;
        while level < self.levels.len() - 1 {
            let from_index = self.levels[level + 1];
            let to_index = self.levels[level];
            for i in from_index..to_index {
                if self.items[i] < value {
                    total += weight;
                } else if (level > 0) || self.is_level_zero_sorted {
                    break; // levels above 0 are sorted, no point comparing further
                }
            }
            level += 1;
            weight *= 2;
        }
        Some(total as f64 / self.count() as f64)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.items.len() == self.items.capacity()
    }

    pub fn is_estimation_mode(&self) -> bool {
        self.levels.len() - 1 > 1
    }

    pub fn count(&self) -> usize {
        self.n
    }

    pub fn retained(&self) -> usize {
        self.levels[0]
    }

    // The following code is only valid in the special case of exactly reaching capacity while updating.
    // It cannot be used while merging, while reducing k, or anything else.
    fn compress_while_updating(&mut self) {
        let level = self.find_level_to_compact();
        self.compact_level(level);
    }

    fn find_level_to_compact(&mut self) -> usize {
        for level in 0..self.levels.len() - 1 {
            let pop = self.levels[level] - self.levels[level + 1];
            let cap = level_capacity(self.k, self.levels.len() - 1, level, self.m);
            if pop >= cap {
                return level;
            }
        }
        panic!("capacity calculation error")
    }

    fn add_empty_top_level_to_completely_full_sketch(&mut self) {
        let new_level_capacity = level_capacity(self.k, self.levels.len() - 1, 0, self.m);
        self.levels.push(0);
        self.items.reserve_exact(new_level_capacity);
        // Because we store the new level at the beginning of the items vector,
        // and it is currently empty, we do not require any adjustments to items.
    }

    fn compact_level(&mut self, level: usize) {
        // If we want to compact the last level, add an empty level.
        if level + 1 == self.levels.len() - 1 {
            self.add_empty_top_level_to_completely_full_sketch();
        }
        let start = self.levels[level + 1];
        let end = self.levels[level];
        let end = if is_odd(end - start) { end - 1 } else { end };
        let mid = (start + end) / 2;
        // Compact level into the lower half of its range.
        randomly_halve_down(&mut self.items[start..end]);
        // Sort the combined level+1.
        self.items[self.levels[level + 2]..mid].sort();
        // Splice out the unused part of items.
        self.items.drain(mid..end);
        // Fix up the level counts.
        let moved = mid - start;
        let removed = end - mid;
        self.levels[level + 1] += moved;
        for i in 0..=level {
            self.levels[i] -= removed;
        }
    }

    fn sort_level_zero(&mut self) {
        self.items[self.levels[1]..].sort()
    }
}

impl<T: Ord + Default> Default for Histogram<T> {
    fn default() -> Self {
        Self::new(DEFAULT_K, DEFAULT_M)
    }
}

fn total_capacity(k: usize, m: usize, num_levels: usize) -> usize {
    let mut total = 0;
    for h in 0..num_levels {
        total += level_capacity(k, num_levels, h, m);
    }
    total
}

fn level_capacity(k: usize, num_levels: usize, height: usize, min_wid: usize) -> usize {
    assert!(height < num_levels);
    let depth = num_levels - height - 1;
    int_cap_aux(k, depth).max(min_wid)
}

fn int_cap_aux(k: usize, depth: usize) -> usize {
    assert!(depth <= 60);
    if depth > 30 {
        return int_cap_aux_aux(k, depth);
    }
    let half = depth / 2;
    let rest = depth - half;
    let tmp = int_cap_aux_aux(k, half);
    int_cap_aux_aux(tmp, rest)
}

fn int_cap_aux_aux(k: usize, depth: usize) -> usize {
    assert!(depth <= 30);
    let twok = k << 1;
    let tmp = (twok << depth) / powers_of_three[depth];
    let result = (tmp + 1) >> 1;
    assert!(result <= k);
    result
}

const powers_of_three: [usize; 31] = [
    1,
    3,
    9,
    27,
    81,
    243,
    729,
    2187,
    6561,
    19683,
    59049,
    177147,
    531441,
    1594323,
    4782969,
    14348907,
    43046721,
    129140163,
    387420489,
    1162261467,
    3486784401,
    10460353203,
    31381059609,
    94143178827,
    282429536481,
    847288609443,
    2541865828329,
    7625597484987,
    22876792454961,
    68630377364883,
    205891132094649,
];

fn is_odd(value: usize) -> bool {
    (value & 1) > 0
}

fn is_even(value: usize) -> bool {
    (value & 1) == 0
}

fn randomly_halve_down<T>(buf: &mut [T]) {
    assert!(is_even(buf.len()));

    let offset = random_bit();
    let mut j = offset;
    let mut i = 0;
    while i < buf.len() / 2 {
        if i != j {
            buf.swap(i, j);
        }
        j += 2;
        i += 1;
    }
}

fn random_bit() -> usize {
    let mut g = rand::thread_rng();
    let bit: bool = g.gen();
    if bit {
        1
    } else {
        0
    }
}

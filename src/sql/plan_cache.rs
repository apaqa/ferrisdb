use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};

use super::optimizer::QueryPlanNode;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
}

#[derive(Debug, Clone)]
struct CachedPlan {
    key: u64,
    tables: Vec<String>,
    plan: QueryPlanNode,
}

#[derive(Debug, Clone)]
pub struct PlanCache {
    capacity: usize,
    entries: HashMap<u64, CachedPlan>,
    order: VecDeque<u64>,
    stats: PlanCacheStats,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            order: VecDeque::new(),
            stats: PlanCacheStats::default(),
        }
    }

    pub fn compute_key(sql: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sql.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get(&mut self, key: u64) -> Option<QueryPlanNode> {
        let plan = self.entries.get(&key).map(|entry| entry.plan.clone());
        if plan.is_some() {
            self.stats.hits += 1;
            self.touch(key);
        } else {
            self.stats.misses += 1;
        }
        plan
    }

    pub fn put(&mut self, key: u64, tables: Vec<String>, plan: QueryPlanNode) {
        if self.entries.contains_key(&key) {
            self.touch(key);
        } else {
            self.order.push_back(key);
        }
        self.entries.insert(
            key,
            CachedPlan {
                key,
                tables,
                plan,
            },
        );
        self.stats.inserts += 1;
        self.evict_if_needed();
    }

    pub fn invalidate_table(&mut self, table: &str) {
        let keys = self
            .entries
            .values()
            .filter(|entry| entry.tables.iter().any(|name| name == table))
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        for key in keys {
            self.entries.remove(&key);
            self.order.retain(|item| *item != key);
        }
    }

    pub fn stats(&self) -> PlanCacheStats {
        self.stats.clone()
    }

    fn touch(&mut self, key: u64) {
        self.order.retain(|item| *item != key);
        self.order.push_back(key);
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
    }
}

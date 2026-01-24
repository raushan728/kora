use crate::{
    error::KoraError,
    signer::config::{SelectionStrategy, SignerConfig, SignerPoolConfig},
};
use rand::Rng;
use solana_keychain::{Signer, SolanaSigner};
use solana_sdk::pubkey::Pubkey;
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

const DEFAULT_WEIGHT: u32 = 1;

/// Metadata associated with a signer in the pool
pub(crate) struct SignerWithMetadata {
    /// Human-readable name for this signer
    name: String,
    /// The actual signer instance
    signer: Arc<Signer>,
    /// Weight for weighted selection (higher = more likely to be selected)
    weight: u32,
    /// Timestamp of last use (Unix timestamp in seconds)
    last_used: AtomicU64,
}

impl Clone for SignerWithMetadata {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            signer: self.signer.clone(),
            weight: self.weight,
            last_used: AtomicU64::new(self.last_used.load(Ordering::Relaxed)),
        }
    }
}

impl SignerWithMetadata {
    /// Create a new signer with metadata
    pub(crate) fn new(name: String, signer: Arc<Signer>, weight: u32) -> Self {
        Self { name, signer, weight, last_used: AtomicU64::new(0) }
    }

    /// Update the last used timestamp to current time
    fn update_last_used(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_used.store(now, Ordering::Relaxed);
    }
}

pub struct SignerPool {
    /// List of signers with their metadata
    signers: Vec<SignerWithMetadata>,
    /// Strategy for selecting signers
    strategy: SelectionStrategy,
    /// Current index for round-robin selection
    current_index: AtomicUsize,
    /// Total weight of all signers in the pool
    total_weight: u32,
    /// Whether transparent failover (retries on other signers) is enabled
    failover_enabled: bool,
}

/// Information about a signer for monitoring/debugging
#[derive(Debug, Clone)]
pub struct SignerInfo {
    pub public_key: String,
    pub name: String,
    pub weight: u32,
    pub last_used: u64, // Unix timestamp
}

impl SignerPool {
    #[cfg(test)]
    pub(crate) fn new(signers: Vec<SignerWithMetadata>) -> Self {
        let total_weight: u32 = signers.iter().map(|s| s.weight).sum();

        Self {
            signers,
            strategy: SelectionStrategy::RoundRobin,
            current_index: AtomicUsize::new(0),
            total_weight,
            failover_enabled: false,
        }
    }

    /// Create a new signer pool from configuration
    pub async fn from_config(config: SignerPoolConfig) -> Result<Self, KoraError> {
        if config.signers.is_empty() {
            return Err(KoraError::ValidationError("Cannot create empty signer pool".to_string()));
        }

        let mut signers = Vec::new();

        for signer_config in config.signers {
            log::info!("Initializing signer: {}", signer_config.name);

            let signer = SignerConfig::build_signer_from_config(&signer_config).await?;
            let weight = signer_config.weight.unwrap_or(DEFAULT_WEIGHT);

            signers.push(SignerWithMetadata::new(signer_config.name, Arc::new(signer), weight));
        }

        let total_weight: u32 = signers.iter().map(|s| s.weight).sum();

        Ok(Self {
            strategy: config.signer_pool.strategy,
            current_index: AtomicUsize::new(0),
            signers,
            total_weight,
            failover_enabled: config.signer_pool.failover_enabled,
        })
    }

    /// Select the next signer index according to the configured strategy (single pick).
    fn select_next_index(&self) -> usize {
        let len = self.signers.len();
        match self.strategy {
            SelectionStrategy::RoundRobin => {
                let idx = self.current_index.fetch_add(1, Ordering::Relaxed).wrapping_add(0);
                (idx % len) as usize
            }
            SelectionStrategy::Random => {
                let mut rng = rand::rng();
                rng.random_range(0..len)
            }
            SelectionStrategy::Weighted => {
                // simple weighted pick
                let mut rng = rand::rng();
                let mut roll = rng.random_range(0..self.total_weight);
                for (i, s) in self.signers.iter().enumerate() {
                    if roll < s.weight {
                        return i;
                    }
                    roll -= s.weight;
                }
                // fallback
                0
            }
        }
    }

    /// Run an operation using a signer chosen by policy. If the operation returns an error,
    /// this helper will only attempt other signers when failover_enabled == true.
    ///
    /// op will be called with a reference to the selected signer.
    pub async fn try_with_failover<T, F, Fut>(&self, mut op: F) -> Result<T, KoraError>
    where
        F: FnMut(&Arc<Signer>) -> Fut,
        Fut: std::future::Future<Output = Result<T, KoraError>>,
    {
        // pick initial signer index deterministically according to strategy
        let start = self.select_next_index();

        // Attempt only the chosen signer if failover disabled
        if !self.failover_enabled {
            let s = &self.signers[start].signer;
            return op(s).await;
        }

        // Otherwise iterate signers (start -> end -> wrap) and return first success
        let len = self.signers.len();
        for i in 0..len {
            let idx = (start + i) % len;
            let s = &self.signers[idx].signer;
            match op(s).await {
                Ok(v) => {
                    // update last used
                    self.signers[idx].update_last_used();
                    return Ok(v);
                }
                Err(_) => {
                    // log and continue to next signer when failover enabled
                    log::warn!(
                        "Signer '{}' failed, attempting next signer (failover enabled)",
                        self.signers[idx].name
                    );
                    continue;
                }
            }
        }

        Err(KoraError::InternalServerError(
            "All signers failed during failover attempts".to_string(),
        ))
    }

    /// Get the next signer according to the configured strategy
    pub fn get_next_signer(&self) -> Result<Arc<Signer>, KoraError> {
        if self.signers.is_empty() {
            return Err(KoraError::InternalServerError("Signer pool is empty".to_string()));
        }
        let idx = self.select_next_index();
        let signer_meta = &self.signers[idx];
        signer_meta.update_last_used();
        Ok(Arc::clone(&signer_meta.signer))
    }

    /// Get information about all signers in the pool
    pub fn get_signers_info(&self) -> Vec<SignerInfo> {
        self.signers
            .iter()
            .map(|s| SignerInfo {
                public_key: s.signer.pubkey().to_string(),
                name: s.name.clone(),
                weight: s.weight,
                last_used: s.last_used.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Get the number of signers in the pool
    pub fn len(&self) -> usize {
        self.signers.len()
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.signers.is_empty()
    }

    /// Get the configured strategy
    pub fn strategy(&self) -> &SelectionStrategy {
        &self.strategy
    }

    /// Get a signer by public key (for client consistency signer keys)
    pub fn get_signer_by_pubkey(&self, pubkey: &str) -> Result<Arc<Signer>, KoraError> {
        // Try to parse as Pubkey to validate format
        let target_pubkey = Pubkey::from_str(pubkey).map_err(|_| {
            KoraError::ValidationError(format!("Invalid signer signer key pubkey: {pubkey}"))
        })?;

        // Find signer with matching public key
        let signer_meta =
            self.signers.iter().find(|s| s.signer.pubkey() == target_pubkey).ok_or_else(|| {
                KoraError::ValidationError(format!("Signer with pubkey {pubkey} not found in pool"))
            })?;

        signer_meta.update_last_used();
        Ok(Arc::clone(&signer_meta.signer))
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::signature::Keypair;

    use super::*;
    use std::collections::HashMap;

    fn create_test_pool() -> SignerPool {
        // Create test signers using external signer library
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();

        let external_signer1 =
            solana_keychain::Signer::from_memory(&keypair1.to_base58_string()).unwrap();
        let external_signer2 =
            solana_keychain::Signer::from_memory(&keypair2.to_base58_string()).unwrap();

        SignerPool {
            signers: vec![
                SignerWithMetadata::new("signer_1".to_string(), Arc::new(external_signer1), 1),
                SignerWithMetadata::new("signer_2".to_string(), Arc::new(external_signer2), 2),
            ],
            strategy: SelectionStrategy::RoundRobin,
            current_index: AtomicUsize::new(0),
            total_weight: 3,
            failover_enabled: false,
        }
    }

    #[test]
    fn test_round_robin_selection() {
        let pool = create_test_pool();

        // Test that round-robin cycles through signers
        let mut selections = HashMap::new();
        for _ in 0..100 {
            let signer = pool.get_next_signer().unwrap();
            *selections.entry(signer.pubkey().to_string()).or_insert(0) += 1;
        }

        // Should have selected both signers equally
        assert_eq!(selections.len(), 2);
        // Each signer should be selected 50 times
        assert!(selections.values().all(|&count| count == 50));
    }

    #[test]
    fn test_weighted_selection() {
        let mut pool = create_test_pool();
        pool.strategy = SelectionStrategy::Weighted;

        let signer1_pubkey = pool.signers[0].signer.pubkey().to_string();
        let signer2_pubkey = pool.signers[1].signer.pubkey().to_string();

        let mut selections = HashMap::new();
        for _ in 0..300 {
            let signer = pool.get_next_signer().unwrap();
            *selections.entry(signer.pubkey().to_string()).or_insert(0) += 1;
        }

        let signer1_count = selections.get(&signer1_pubkey).unwrap_or(&0);
        let signer2_count = selections.get(&signer2_pubkey).unwrap_or(&0);

        // signer_2 has weight 2, signer_1 has weight 1
        assert!(*signer2_count > *signer1_count);
        assert!(*signer2_count > 150);
        assert!(*signer1_count > 50);
    }

    #[test]
    fn test_empty_pool() {
        let pool = SignerPool {
            signers: vec![],
            strategy: SelectionStrategy::RoundRobin,
            current_index: AtomicUsize::new(0),
            total_weight: 0,
            failover_enabled: false,
        };

        assert!(pool.get_next_signer().is_err());
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[tokio::test]
    async fn test_try_with_failover_disabled() {
        let pool = create_test_pool();
        // failover_enabled is false by default in create_test_pool

        // Create an op that always fails
        let result: Result<(), _> = pool
            .try_with_failover(|_s| async {
                Err(KoraError::SigningError("Always fail".to_string()))
            })
            .await;

        // Should return the error (no retry logic triggered)
        assert!(result.is_err());
        match result.unwrap_err() {
            KoraError::SigningError(msg) => assert_eq!(msg, "Always fail"),
            _ => panic!("Unexpected error type"),
        }
    }

    #[tokio::test]
    async fn test_try_with_failover_enabled_success() {
        let mut pool = create_test_pool();
        pool.failover_enabled = true;

        // Create an op that fails for first signer but succeeds for second
        let first_signer_pubkey = pool.signers[0].signer.pubkey();

        let result = pool
            .try_with_failover(|s| {
                let s_pubkey = s.pubkey();
                async move {
                    if s_pubkey == first_signer_pubkey {
                        Err(KoraError::SigningError("First signer fail".to_string()))
                    } else {
                        Ok("Success".to_string())
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Success");
    }

    #[tokio::test]
    async fn test_try_with_failover_enabled_all_fail() {
        let mut pool = create_test_pool();
        pool.failover_enabled = true;

        let result: Result<(), _> = pool
            .try_with_failover(|_s| async {
                Err(KoraError::SigningError("Always fail".to_string()))
            })
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            KoraError::InternalServerError(msg) => assert!(msg.contains("All signers failed")),
            _ => panic!("Unexpected error type"),
        }
    }
}

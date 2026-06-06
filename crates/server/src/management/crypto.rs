// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared cryptographic helpers for the management API.
//!
//! Provides secret key generation and AES-256-GCM encryption used by both
//! permanent access keys (`iam_user_self`) and temporary session credentials
//! (`assume_role`).

use rand::Rng;

/// Generate a 40-character secret access key branded with `extenddb` prefix.
///
/// Format: `extenddb` + 32 random chars = 40 total.
pub fn generate_secret_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    let suffix: String = (0..32)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect();
    format!("extenddb{suffix}")
}

/// Encrypt a secret key with AES-256-GCM using the base64-encoded encryption key.
///
/// `aad` is Associated Authenticated Data (e.g., the access key ID) that binds
/// the ciphertext to its context, preventing ciphertext from being moved between
/// records (CB-11).
///
/// Returns `nonce || ciphertext` as a single byte vector.
pub fn encrypt_secret(plaintext: &str, key_b64: &str, aad: &str) -> Result<Vec<u8>, String> {
    use aes_gcm::Aes256Gcm;
    use aes_gcm::KeyInit;
    use aes_gcm::aead::Aead;
    use aes_gcm::aead::Payload;
    use base64::Engine;

    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|e| format!("decode encryption key: {e}"))?;

    let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    // Generate random 12-byte nonce.
    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);

    let payload = Payload {
        msg: plaintext.as_bytes(),
        aad: aad.as_bytes(),
    };
    let ciphertext = cipher
        .encrypt(nonce, payload)
        .map_err(|e| format!("encrypt: {e}"))?;

    // Prepend nonce to ciphertext for storage.
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Generate a 20-character session access key ID branded with `ASIAEXTENDDB` prefix.
///
/// Format: `ASIAEXTENDDB` + 8 random alphanumeric chars = 20 total.
/// Used by `assume_role` for temporary credentials.
pub fn generate_session_key_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..8)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect();
    format!("ASIAEXTENDDB{suffix}")
}

/// Generate a session token (64-character random string).
///
/// Used by `assume_role` for temporary session credentials.
pub fn generate_session_token() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    (0..64)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

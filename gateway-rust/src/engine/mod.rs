//! 注文処理エンジン
//!
//! gateway-core のキュー・リスクチェック・メトリクスを使用して
//! 注文を処理する。

mod fast_path;

pub use fast_path::{FastPathEngine, ProcessResult};

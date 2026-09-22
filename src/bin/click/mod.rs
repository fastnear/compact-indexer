mod utils;

use serde_repr::{Deserialize_repr, Serialize_repr};
pub use utils::extract_rows;

use serde::Serialize;

const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum ReceiptStatus {
    Failure = 1,
    Success = 2,
}

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum ActionKind {
    CreateAccount = 1,
    DeployContract = 2,
    FunctionCall = 3,
    Transfer = 4,
    Stake = 5,
    AddKey = 6,
    DeleteKey = 7,
    DeleteAccount = 8,
    Delegate = 9,
    DeployGlobalContract = 10,
    DeployGlobalContractByAccountId = 11,
    UseGlobalContract = 12,
    UseGlobalContractByAccountId = 13,
    DeterministicStateInit = 14,
    TransferToGasKey = 15,
    WithdrawFromGasKey = 16,
    DelegateV2 = 17,
}

#[derive(Serialize)]
pub struct ActionRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub receipt_index: u16,
    pub action_index: u8,
    pub signer_id: String,
    pub signer_public_key: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ReceiptStatus,
    pub action: ActionKind,
    pub contract_hash: Option<String>,
    pub public_key: Option<String>,
    pub access_key_contract_id: Option<String>,
    pub deposit: Option<u128>,
    pub gas_price: u128,
    pub attached_gas: Option<u64>,
    pub gas_burnt: u64,
    pub tokens_burnt: u128,
    pub method_name: Option<String>,
    pub args_account_id: Option<String>,
    pub args_new_account_id: Option<String>,
    pub args_owner_id: Option<String>,
    pub args_receiver_id: Option<String>,
    pub args_sender_id: Option<String>,
    pub args_token_id: Option<String>,
    pub args_amount: Option<u128>,
    pub args_balance: Option<u128>,
    pub args_nft_contract_id: Option<String>,
    pub args_nft_token_id: Option<String>,
    pub args_utm_source: Option<String>,
    pub args_utm_medium: Option<String>,
    pub args_utm_campaign: Option<String>,
    pub args_utm_term: Option<String>,
    pub args_utm_content: Option<String>,
    pub return_value_int: Option<u128>,
}

#[derive(Serialize)]
pub struct EventRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub receipt_index: u16,
    pub log_index: u16,
    pub signer_id: String,
    pub signer_public_key: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ReceiptStatus,

    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
    pub data_account_id: Option<String>,
    pub data_owner_id: Option<String>,
    pub data_old_owner_id: Option<String>,
    pub data_new_owner_id: Option<String>,
    pub data_liquidation_account_id: Option<String>,
    pub data_authorized_id: Option<String>,
    pub data_token_ids: Vec<String>,
    pub data_token_id: Option<String>,
    pub data_position: Option<String>,
    pub data_amount: Option<u128>,
}

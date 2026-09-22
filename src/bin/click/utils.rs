use crate::click::{ActionKind, ActionRow, EventRow, ReceiptStatus, EVENT_LOG_PREFIX};
use fastnear_primitives::block_with_tx_hash::BlockWithTxHashes;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::types::AccountId;
use fastnear_primitives::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView,
    ReceiptEnumView, ReceiptView,
};
use serde::Deserialize;

pub fn extract_return_value_int(execution_status: ExecutionStatusView) -> Option<u128> {
    if let ExecutionStatusView::SuccessValue(value) = execution_status {
        let str_value = serde_json::from_slice::<String>(&value).ok()?;
        str_value.parse::<u128>().ok()
    } else {
        None
    }
}

#[derive(Deserialize)]
pub struct ArgsData {
    pub account_id: Option<AccountId>,
    pub args_new_account_id: Option<AccountId>,
    pub args_owner_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub sender_id: Option<AccountId>,
    pub token_id: Option<String>,
    pub nft_contract_id: Option<AccountId>,
    pub nft_token_id: Option<String>,
    pub amount: Option<String>,
    pub balance: Option<String>,
    // UTM
    #[serde(rename = "_utm_source")]
    pub utm_source: Option<String>,
    #[serde(rename = "_utm_medium")]
    pub utm_medium: Option<String>,
    #[serde(rename = "_utm_campaign")]
    pub utm_campaign: Option<String>,
    #[serde(rename = "_utm_term")]
    pub utm_term: Option<String>,
    #[serde(rename = "_utm_content")]
    pub utm_content: Option<String>,
}

const MAX_TOKEN_LENGTH: usize = 64;
const MAX_TOKEN_IDS_LENGTH: usize = 4;

fn limit_length(s: &mut Option<String>) {
    if s.as_ref().map(|s| s.len()).unwrap_or(0) > MAX_TOKEN_LENGTH {
        *s = None;
    }
}

pub fn extract_args_data(action: &ActionView) -> Option<ArgsData> {
    match action {
        ActionView::FunctionCall { args, .. } => {
            let mut args_data: ArgsData = serde_json::from_slice(&args).ok()?;
            // If token length is larger than 64 bytes, we remove it.
            limit_length(&mut args_data.token_id);
            limit_length(&mut args_data.nft_token_id);
            limit_length(&mut args_data.utm_source);
            limit_length(&mut args_data.utm_medium);
            limit_length(&mut args_data.utm_campaign);
            limit_length(&mut args_data.utm_term);
            limit_length(&mut args_data.utm_content);
            Some(args_data)
        }
        _ => None,
    }
}

#[derive(Deserialize)]
pub struct EventData {
    pub account_id: Option<AccountId>,
    pub owner_id: Option<AccountId>,
    pub old_owner_id: Option<AccountId>,
    pub new_owner_id: Option<AccountId>,
    pub liquidation_account_id: Option<AccountId>,
    pub authorized_id: Option<AccountId>,
    pub token_ids: Option<Vec<String>>,
    pub token_id: Option<String>,
    pub position: Option<String>,
    pub amount: Option<String>,
}

#[derive(Deserialize)]
pub struct Event {
    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
    pub data: Option<Vec<EventData>>,
}

pub fn parse_event(event: &str) -> Option<Event> {
    let mut event: Event = serde_json::from_str(&event).ok()?;
    limit_length(&mut event.version);
    limit_length(&mut event.standard);
    limit_length(&mut event.event);
    if let Some(data) = event.data.as_mut().and_then(|data| data.get_mut(0)) {
        if let Some(token_ids) = data.token_ids.as_mut() {
            token_ids.retain(|s| s.len() <= MAX_TOKEN_LENGTH);
            if token_ids.len() > MAX_TOKEN_IDS_LENGTH {
                token_ids.resize(MAX_TOKEN_IDS_LENGTH, "".to_string());
            }
        }
        limit_length(&mut data.token_id);
    } else {
        event.data = None;
    }
    Some(event)
}

pub fn extract_rows(msg: BlockWithTxHashes) -> (Vec<ActionRow>, Vec<EventRow>) {
    let mut action_rows = vec![];
    let mut event_rows = vec![];

    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;

    let mut receipt_index: u16 = 0;
    for shard in msg.shards {
        for outcome in shard.receipt_execution_outcomes {
            let ReceiptView {
                predecessor_id,
                receiver_id: account_id,
                receipt_id,
                receipt,
                _priority,
            } = outcome.receipt;
            let predecessor_id = predecessor_id.to_string();
            let account_id = account_id.to_string();
            let receipt_id = receipt_id.to_string();
            let ExecutionOutcomeView {
                status: execution_status,
                gas_burnt,
                tokens_burnt,
                logs,
                ..
            } = outcome.execution_outcome.outcome;
            let tokens_burnt = tokens_burnt.as_yoctonear();
            let gas_burnt = gas_burnt.as_gas();
            let status = match &execution_status {
                ExecutionStatusView::Unknown => ReceiptStatus::Failure,
                ExecutionStatusView::Failure(_) => ReceiptStatus::Failure,
                ExecutionStatusView::SuccessValue(_) => ReceiptStatus::Success,
                ExecutionStatusView::SuccessReceiptId(_) => ReceiptStatus::Success,
            };
            let return_value_int = extract_return_value_int(execution_status);
            match receipt {
                ReceiptEnumView::Action {
                    signer_id,
                    signer_public_key,
                    actions,
                    gas_price,
                    ..
                } => {
                    let gas_price = gas_price.as_yoctonear();
                    for (log_index, log) in logs.into_iter().enumerate() {
                        if log.starts_with(EVENT_LOG_PREFIX) {
                            let log_index = u16::try_from(log_index).expect("Log index overflow");
                            let event = parse_event(&log.as_str()[EVENT_LOG_PREFIX.len()..]);
                            if let Some(mut event) = event {
                                let data = event.data.take().map(|mut data| data.remove(0));
                                if let Some(data) = data {
                                    event_rows.push(EventRow {
                                        block_height,
                                        block_hash: block_hash.clone(),
                                        block_timestamp,
                                        receipt_id: receipt_id.clone(),
                                        receipt_index,
                                        log_index,
                                        signer_id: signer_id.to_string(),
                                        signer_public_key: signer_public_key.to_string(),
                                        predecessor_id: predecessor_id.clone(),
                                        account_id: account_id.clone(),
                                        status,

                                        version: event.version,
                                        standard: event.standard,
                                        event: event.event,
                                        data_account_id: data
                                            .account_id
                                            .as_ref()
                                            .map(|account_id| account_id.to_string()),
                                        data_owner_id: data
                                            .owner_id
                                            .as_ref()
                                            .map(|owner_id| owner_id.to_string()),
                                        data_old_owner_id: data
                                            .old_owner_id
                                            .as_ref()
                                            .map(|old_owner_id| old_owner_id.to_string()),
                                        data_new_owner_id: data
                                            .new_owner_id
                                            .as_ref()
                                            .map(|new_owner_id| new_owner_id.to_string()),
                                        data_liquidation_account_id: data
                                            .liquidation_account_id
                                            .as_ref()
                                            .map(|liquidation_account_id| {
                                                liquidation_account_id.to_string()
                                            }),
                                        data_authorized_id: data
                                            .authorized_id
                                            .as_ref()
                                            .map(|authorized_id| authorized_id.to_string()),
                                        data_token_ids: data.token_ids.clone().unwrap_or_default(),
                                        data_token_id: data.token_id,
                                        data_position: data.position,
                                        data_amount: data
                                            .amount
                                            .as_ref()
                                            .and_then(|amount| amount.parse().ok()),
                                    });
                                }
                            }
                        }
                    }

                    for (action_index, action) in actions.into_iter().enumerate() {
                        let action_index =
                            u8::try_from(action_index).expect("Action index overflow");
                        let args_data = extract_args_data(&action);
                        let row = ActionRow {
                            block_height,
                            block_hash: block_hash.clone(),
                            block_timestamp,
                            receipt_id: receipt_id.clone(),
                            receipt_index,
                            action_index,
                            signer_id: signer_id.to_string(),
                            signer_public_key: signer_public_key.to_string(),
                            predecessor_id: predecessor_id.clone(),
                            account_id: account_id.clone(),
                            status,
                            action: match action {
                                ActionView::CreateAccount => ActionKind::CreateAccount,
                                ActionView::DeployContract { .. } => ActionKind::DeployContract,
                                ActionView::FunctionCall { .. } => ActionKind::FunctionCall,
                                ActionView::Transfer { .. } => ActionKind::Transfer,
                                ActionView::Stake { .. } => ActionKind::Stake,
                                ActionView::AddKey { .. } => ActionKind::AddKey,
                                ActionView::DeleteKey { .. } => ActionKind::DeleteKey,
                                ActionView::DeleteAccount { .. } => ActionKind::DeleteAccount,
                                ActionView::Delegate { .. } => ActionKind::Delegate,
                                ActionView::DeployGlobalContract { .. } => {
                                    ActionKind::DeployGlobalContract
                                }
                                ActionView::DeployGlobalContractByAccountId { .. } => {
                                    ActionKind::DeployGlobalContractByAccountId
                                }
                                ActionView::UseGlobalContract { .. } => {
                                    ActionKind::UseGlobalContract
                                }
                                ActionView::UseGlobalContractByAccountId { .. } => {
                                    ActionKind::UseGlobalContractByAccountId
                                }
                                ActionView::DeterministicStateInit { .. } => {
                                    ActionKind::DeterministicStateInit
                                }
                                ActionView::TransferToGasKey { .. } => ActionKind::TransferToGasKey,
                                ActionView::WithdrawFromGasKey { .. } => {
                                    ActionKind::WithdrawFromGasKey
                                }
                                ActionView::DelegateV2 { .. } => ActionKind::DelegateV2,
                            },
                            contract_hash: match &action {
                                ActionView::DeployContract { code } => {
                                    Some(CryptoHash::hash_bytes(&code).to_string())
                                }
                                _ => None,
                            },
                            public_key: match &action {
                                ActionView::AddKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                ActionView::DeleteKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                ActionView::TransferToGasKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                ActionView::WithdrawFromGasKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                _ => None,
                            },
                            access_key_contract_id: match &action {
                                ActionView::AddKey { access_key, .. } => {
                                    match &access_key.permission {
                                        AccessKeyPermissionView::FunctionCall {
                                            receiver_id,
                                            ..
                                        } => Some(receiver_id.to_string()),
                                        _ => None,
                                    }
                                }
                                _ => None,
                            },
                            deposit: match &action {
                                ActionView::Transfer { deposit, .. } => Some(*deposit),
                                ActionView::Stake { stake, .. } => Some(*stake),
                                ActionView::FunctionCall { deposit, .. } => Some(*deposit),
                                ActionView::DeterministicStateInit { deposit, .. } => {
                                    Some(*deposit)
                                }
                                ActionView::TransferToGasKey { deposit, .. } => Some(*deposit),
                                ActionView::WithdrawFromGasKey { amount, .. } => Some(*amount),
                                _ => None,
                            }
                            .map(|d| d.as_yoctonear()),
                            gas_price,
                            attached_gas: match &action {
                                ActionView::FunctionCall { gas, .. } => Some(*gas),
                                _ => None,
                            }
                            .map(|g| g.as_gas()),
                            gas_burnt,
                            tokens_burnt,
                            method_name: match &action {
                                ActionView::FunctionCall { method_name, .. } => {
                                    Some(method_name.to_string())
                                }
                                _ => None,
                            },
                            args_account_id: args_data.as_ref().and_then(|args| {
                                args.account_id
                                    .as_ref()
                                    .map(|account_id| account_id.to_string())
                            }),
                            args_new_account_id: args_data.as_ref().and_then(|args| {
                                args.args_new_account_id
                                    .as_ref()
                                    .map(|new_account_id| new_account_id.to_string())
                            }),
                            args_owner_id: args_data.as_ref().and_then(|args| {
                                args.args_owner_id
                                    .as_ref()
                                    .map(|owner_id| owner_id.to_string())
                            }),
                            args_receiver_id: args_data.as_ref().and_then(|args| {
                                args.receiver_id
                                    .as_ref()
                                    .map(|receiver_id| receiver_id.to_string())
                            }),
                            args_sender_id: args_data.as_ref().and_then(|args| {
                                args.sender_id
                                    .as_ref()
                                    .map(|sender_id| sender_id.to_string())
                            }),
                            args_token_id: args_data
                                .as_ref()
                                .and_then(|args| args.token_id.clone()),
                            args_amount: args_data.as_ref().and_then(|args| {
                                args.amount.as_ref().and_then(|amount| amount.parse().ok())
                            }),
                            args_balance: args_data.as_ref().and_then(|args| {
                                args.balance
                                    .as_ref()
                                    .and_then(|balance| balance.parse().ok())
                            }),
                            args_nft_contract_id: args_data.as_ref().and_then(|args| {
                                args.nft_contract_id
                                    .as_ref()
                                    .map(|nft_contract_id| nft_contract_id.to_string())
                            }),
                            args_nft_token_id: args_data.as_ref().and_then(|args| {
                                args.nft_token_id
                                    .as_ref()
                                    .map(|nft_token_id| nft_token_id.to_string())
                            }),
                            args_utm_source: args_data.as_ref().and_then(|args| {
                                args.utm_source
                                    .as_ref()
                                    .map(|utm_source| utm_source.to_string())
                            }),
                            args_utm_medium: args_data.as_ref().and_then(|args| {
                                args.utm_medium
                                    .as_ref()
                                    .map(|utm_medium| utm_medium.to_string())
                            }),
                            args_utm_campaign: args_data.as_ref().and_then(|args| {
                                args.utm_campaign
                                    .as_ref()
                                    .map(|utm_campaign| utm_campaign.to_string())
                            }),
                            args_utm_term: args_data.as_ref().and_then(|args| {
                                args.utm_term.as_ref().map(|utm_term| utm_term.to_string())
                            }),
                            args_utm_content: args_data.as_ref().and_then(|args| {
                                args.utm_content
                                    .as_ref()
                                    .map(|utm_content| utm_content.to_string())
                            }),
                            return_value_int,
                        };
                        action_rows.push(row);
                    }
                }
                ReceiptEnumView::Data { .. } => {}
                ReceiptEnumView::GlobalContractDistribution { .. } => {}
            }
            receipt_index = receipt_index
                .checked_add(1)
                .expect("Receipt index overflow");
        }
    }
    (action_rows, event_rows)
}

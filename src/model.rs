use std::fmt::Display;

pub trait EventBody: std::fmt::Debug {}

#[allow(unused)]
#[derive(Debug)]
pub struct EventHeader {
    pub timestamp: u32,
    pub type_code: u8,
    pub server_id: u32,
    pub event_length: u32,
    pub next_event_position: u32,
    pub flags: u16,
}

#[allow(unused)]
#[derive(Debug)]
/// format description
pub struct EventBodyTypeCode15 {
    pub binlog_version: u16,
    pub server_version: String,
    pub create_timestamp: u32,
    pub header_length: u8,
}

impl EventBody for EventBodyTypeCode15 {}

#[allow(unused)]
#[derive(Debug)]
/// annotate row
/// sql text
pub struct EventBodyTypeCode160 {
    pub sql: String,
}

impl EventBody for EventBodyTypeCode160 {}

#[allow(unused)]
#[derive(Debug)]
/// gtid list
pub struct EventBodyTypeCode163 {
    pub number_of_gtids: u32,
    pub gtids: Vec<GTID>,
}

#[allow(unused)]
#[derive(Debug)]
pub struct GTID {
    pub replication_domain_id: u32,
    pub server_id: u32,
    pub gtid_sequence: u64,
}

impl EventBody for EventBodyTypeCode163 {}

#[allow(unused)]
#[derive(Debug, Clone)]
/// table map
pub struct EventBodyTypeCode19 {
    // 这里只要6字节，只能向上取到u64
    pub table_id: u64,

    pub reserved_for_future_use: u16,
    pub database_name_length: u8,
    pub database_name: String,
    pub table_name_length: u8,
    pub table_name: String,
    pub number_of_columns: u64,
    pub column_types: Vec<u8>,
    pub column_types_string_for_human: Vec<String>,
    pub number_of_metadata_block: u64,
    pub metadata_block: Vec<u8>,
    pub metadata_block_string_for_human: Vec<String>,
    pub metadata_block_data_raw: Vec<Vec<u8>>,
    pub columns_can_be_null: Vec<bool>,
    pub optional_metadata_block: Vec<u8>,
}

impl EventBody for EventBodyTypeCode19 {}

#[allow(unused)]
#[derive(Debug)]
/// xid
pub struct EventBodyTypeCode16 {
    pub xid_transaction_number: u8,
}

impl EventBody for EventBodyTypeCode16 {}

#[allow(unused)]
#[derive(Debug)]
/// query
pub struct EventBodyTypeCode2 {
    pub id_of_thread: u32,
    pub execute_time: u32,
    pub length_of_database_name: u8,
    pub error_code: u16,
    pub length_of_status_variable_block: u16,
    pub status_variables: Vec<u8>,
    pub status_variables_string_vec_for_human: Vec<String>,
    pub database_name: String,
    pub sql: String,
}

impl EventBody for EventBodyTypeCode2 {}

#[allow(unused)]
#[derive(Debug)]
/// binlog_checkpoint
pub struct EventBodyTypeCode161 {
    pub log_filename_length: u32,
    pub log_filename: String,
}

impl EventBody for EventBodyTypeCode161 {}

#[allow(unused)]
#[derive(Debug)]
/// gtid event
pub struct EventBodyTypeCode162 {
    pub gtid_sequence: u64,
    pub replication_domain_id: u32,
    pub flags: u8,
    pub commit_id: Option<u64>,
    pub format_id: Option<u32>,
    pub gtid_length: Option<u8>,
    pub bqual_length: Option<u8>,
    pub xid: Option<Vec<u8>>,
}

impl EventBody for EventBodyTypeCode162 {}

#[allow(unused)]
#[derive(Debug)]
/// intvar event
pub struct EventBodyTypeCode5 {
    pub data_type: u8,
    pub value: u64,
}

impl EventBody for EventBodyTypeCode5 {}

#[allow(unused)]
#[derive(Debug)]
/// rotate event
pub struct EventBodyTypeCode4 {
    pub position_of_the_first_event_in_next_log_file: u64,
    pub file_name_of_next_binary_log: String,
}

impl EventBody for EventBodyTypeCode4 {}

#[allow(unused)]
#[derive(Debug)]
/// insert update delete event
pub struct EventBodyTypeCode23To25 {
    pub type_string_for_human: String,
    pub table_id: u64,
    pub flags: u16,
    pub number_of_columns: u64,
    pub columns_used: Vec<bool>,
    pub columns_used_for_update: Option<Vec<bool>>,
    pub null_bitmap: Vec<bool>,
    pub column_data: Vec<String>,
    pub null_bitmap_for_update: Option<Vec<bool>>,
    pub column_data_for_update: Option<Vec<String>>,
}

impl EventBody for EventBodyTypeCode23To25 {}

#[allow(unused)]
#[derive(Debug)]
/// rand event
pub struct EventBodyTypeCode13 {
    pub first_seed: u64,
    pub second_seed: u64,
}

impl EventBody for EventBodyTypeCode13 {}

#[allow(unused)]
#[derive(Debug)]
/// start encryption event
pub struct EventBodyTypeCode164 {
    pub encryption_scheme: u8,
    pub encryption_key_version: u32,
    pub nonce: Vec<u8>,
}

impl EventBody for EventBodyTypeCode164 {}

#[allow(unused)]
#[derive(Debug)]
/// XA prepare log event
pub struct EventBodyTypeCode38 {
    pub one_phase_commit: u8,
    pub format_id: u32,
    pub length_of_gtrid: u32,
    pub length_of_bqual: u8,
    pub xid: Vec<u8>,
}

impl EventBody for EventBodyTypeCode38 {}

#[allow(unused)]
#[derive(Debug)]
/// user var event
pub struct EventBodyTypeCode14 {
    pub length_of_user_variable_name: u32,
    pub name_of_user_variable: String,
    pub null_indicator: u8,
    pub variable_type: Option<u8>,
    pub variable_type_string_for_human: Option<String>,
    pub collation_number: Option<u32>,
    pub length_of_value: Option<u32>,
    pub value: Option<String>,
    pub flags: Option<u8>,
}

impl EventBody for EventBodyTypeCode14 {}

#[allow(unused)]
#[derive(Debug)]
pub struct EventBodyTypeSkip(pub u8);

impl EventBody for EventBodyTypeSkip {}

#[derive(Debug)]
pub struct MyError(pub String);

impl std::error::Error for MyError {}

impl Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "some error occoured: {}", self.0)
    }
}

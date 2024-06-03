use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek},
};

use crate::model::*;
use crate::util::*;

const EVENT_HEADER_LENGTH: usize = 19;

type BoxedError = Box<dyn std::error::Error>;

pub fn get_event_header(file: &mut File, offset: u64) -> Result<EventHeader, BoxedError> {
    let mut buffer = [0u8; EVENT_HEADER_LENGTH as usize];

    file.seek(std::io::SeekFrom::Start(offset))?;

    file.read_exact(&mut buffer)?;

    let event_header = EventHeader {
        timestamp: { u32::from_le_bytes(buffer[0..4].try_into()?) },
        type_code: { u8::from_le_bytes(buffer[4..5].try_into()?) },
        server_id: { u32::from_le_bytes(buffer[5..9].try_into()?) },
        event_length: { u32::from_le_bytes(buffer[9..13].try_into()?) },
        next_event_position: { u32::from_le_bytes(buffer[13..17].try_into()?) },
        flags: { u16::from_le_bytes(buffer[17..19].try_into()?) },
    };

    Ok(event_header)
}

pub fn get_event_body(
    file: &mut File,
    offset: u64,
    event_length: u32,
    type_code: u8,
    table_structs: &mut HashMap<u64, EventBodyTypeCode19>,
) -> Result<Box<dyn EventBody>, BoxedError> {
    let body_length = (event_length - EVENT_HEADER_LENGTH as u32) as usize;
    let mut buffer = vec![0u8; body_length];

    file.seek(std::io::SeekFrom::Start(offset))?;

    file.read_exact(&mut buffer)?;

    if cfg!(feature = "test") {
        let event_body: Result<Box<dyn EventBody>, BoxedError> = match type_code {
            2 => deal_type_code_2(buffer),
            5 => deal_type_code_5(buffer),
            4 => deal_type_code_4(buffer),
            15 => deal_type_code_15(buffer),
            16 => deal_type_code_16(buffer),
            23..=25 => deal_type_code_23_to_25(buffer, type_code, table_structs),
            160 => deal_type_code_160(buffer),
            163 => deal_type_code_163(buffer),
            19 => deal_type_code_19(buffer, table_structs),
            161 => deal_type_code_161(buffer),
            162 => deal_type_code_162(buffer),
            _ => Ok(Box::new(EventBodyTypeSkip(type_code))),
        };

        let event_body = event_body?;

        Ok(event_body)
    } else {
        let event_body: Result<Box<dyn EventBody>, BoxedError> = match type_code {
            2 => deal_type_code_2(buffer),
            5 => deal_type_code_5(buffer),
            4 => deal_type_code_4(buffer),
            13 => deal_type_code_13(buffer),
            14 => deal_type_code_14(buffer),
            15 => deal_type_code_15(buffer),
            16 => deal_type_code_16(buffer),
            19 => deal_type_code_19(buffer, table_structs),
            23..=25 => deal_type_code_23_to_25(buffer, type_code, table_structs),
            38 => deal_type_code_38(buffer),
            160 => deal_type_code_160(buffer),
            161 => deal_type_code_161(buffer),
            162 => deal_type_code_162(buffer),
            163 => deal_type_code_163(buffer),
            164 => deal_type_code_164(buffer),
            _ => Ok(Box::new(EventBodyTypeSkip(type_code))),
        };

        let event_body = event_body?;

        Ok(event_body)
    }
}

pub fn deal_type_code_15(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let event_body = EventBodyTypeCode15 {
        binlog_version: { u16::from_le_bytes(buffer[0..2].try_into()?) },
        server_version: {
            String::from_utf8(buffer[2..52].try_into()?)?
                .trim_end_matches(char::from(0))
                .to_string()
        },
        create_timestamp: { u32::from_le_bytes(buffer[52..56].try_into()?) },
        header_length: u8::from_be_bytes(buffer[56..57].try_into()?),
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_160(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let buffer_length = buffer.len();
    // 这里做掉的4byte是CRC32
    let event_body = EventBodyTypeCode160 {
        sql: { String::from_utf8(buffer[0..buffer_length - 4].try_into()?)? },
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_163(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let number_of_gtids = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
    offset += 4;

    let mut gtids = Vec::new();

    for _ in 0..number_of_gtids {
        gtids.push(GTID {
            replication_domain_id: { u32::from_le_bytes(buffer[offset..offset + 4].try_into()?) },
            server_id: {
                offset += 4;
                u32::from_le_bytes(buffer[offset..offset + 4].try_into()?)
            },
            gtid_sequence: {
                offset += 4;
                u64::from_le_bytes(buffer[offset..offset + 8].try_into()?)
            },
        });

        offset += 8;
    }

    let event_body = EventBodyTypeCode163 {
        number_of_gtids: number_of_gtids,
        gtids: gtids,
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_19(
    buffer: Vec<u8>,
    table_structs: &mut HashMap<u64, EventBodyTypeCode19>,
) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let mut buffer_for_table_name = buffer[offset..offset + 6].to_vec();
    buffer_for_table_name.splice(
        buffer_for_table_name.len()..buffer_for_table_name.len(),
        vec![0, 0],
    );

    let table_id = u64::from_le_bytes(buffer_for_table_name.as_slice().try_into()?);
    offset += 6;

    let reserved_for_future_use = u16::from_le_bytes(buffer[offset..offset + 2].try_into()?);
    offset += 2;

    let database_name_length = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let database_name =
        String::from_utf8(buffer[offset..offset + database_name_length as usize].to_vec())?;
    offset += database_name_length as usize;
    // 这里多加一个1是因为他是以null结尾的
    offset += 1;

    let table_name_length = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let table_name =
        String::from_utf8(buffer[offset..offset + table_name_length as usize].to_vec())?;
    offset += table_name_length as usize;
    // 这里多加一个1是因为他是以null结尾的
    offset += 1;

    let (number_of_columns, skip_bytes) = parse_lenenc(&buffer[offset..])?;
    offset += skip_bytes as usize;

    let column_types = buffer[offset..offset + number_of_columns as usize].to_vec();
    offset += number_of_columns as usize;

    let mut column_types_string_for_human = Vec::new();
    let column_types_mapping = get_field_types_mapping()?;
    for column_type in &column_types {
        column_types_string_for_human
            .push(column_types_mapping.get(column_type).unwrap().to_string());
    }

    let (number_of_metadata_block, skip_bytes) = parse_lenenc(&buffer[offset..])?;
    offset += skip_bytes as usize;

    let metadata_block = buffer[offset..offset + number_of_metadata_block as usize].to_vec();
    offset += number_of_metadata_block as usize;

    let mut metadata_block_string_for_human = Vec::new();
    let mut metadata_block_data_raw = Vec::new();
    let metadata_block_mapping: HashMap<u8, u8> = get_metadata_block_mapping()?;
    let mut metadata_block_offset = 0;
    for column_type in &column_types {
        let (metadata_block_for_human, metadata_block_raw, skip) = parse_metadata_block(
            &metadata_block_mapping,
            &column_types_mapping,
            &metadata_block,
            metadata_block_offset,
            *column_type,
        )?;

        if metadata_block_for_human.is_empty() {
            continue;
        }

        metadata_block_offset += skip;

        metadata_block_string_for_human.push(metadata_block_for_human);
        metadata_block_data_raw.push(metadata_block_raw);
    }

    let columns_can_be_null_byte_vec_length = (number_of_columns + 7) / 8;

    let columns_can_be_null = parse_bitmap(
        &buffer[offset..offset + columns_can_be_null_byte_vec_length as usize],
        number_of_columns,
    );

    let optional_metadata_block = buffer[offset..].to_vec();

    let event_body = EventBodyTypeCode19 {
        table_id: table_id,
        reserved_for_future_use: reserved_for_future_use,
        database_name_length: database_name_length,
        database_name: database_name,
        table_name_length: table_name_length,
        table_name: table_name,
        number_of_columns: number_of_columns,
        column_types: column_types,
        column_types_string_for_human: column_types_string_for_human,
        number_of_metadata_block: number_of_metadata_block,
        metadata_block: metadata_block,
        metadata_block_string_for_human: metadata_block_string_for_human,
        metadata_block_data_raw: metadata_block_data_raw,
        columns_can_be_null: columns_can_be_null,
        optional_metadata_block: optional_metadata_block,
    };

    let a = table_structs.entry(table_id).or_insert(event_body.clone());
    *a = event_body.clone();

    Ok(Box::new(event_body))
}

pub fn deal_type_code_16(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let offset = 0;

    let xid_transaction_number = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);

    let event_body = EventBodyTypeCode16 {
        xid_transaction_number: xid_transaction_number,
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_2(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let id_of_thread = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
    offset += 4;

    let execute_time = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
    offset += 4;

    let length_of_database_name = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let error_code = u16::from_le_bytes(buffer[offset..offset + 2].try_into()?);
    offset += 2;

    let length_of_status_variable_block =
        u16::from_le_bytes(buffer[offset..offset + 2].try_into()?);
    offset += 2;

    let status_variables;
    let status_variables_string_vec_for_human;
    if length_of_status_variable_block > 0 {
        status_variables =
            buffer[offset..offset + length_of_status_variable_block as usize].to_vec();
        status_variables_string_vec_for_human = parse_status_variables(&status_variables)?;
        offset += length_of_status_variable_block as usize;
    } else {
        status_variables = Vec::new();
        status_variables_string_vec_for_human = Vec::new();
    }

    // 这里多加1是因为尾部的\0
    let database_name = String::from_utf8(
        buffer[offset..offset + length_of_database_name as usize + 1].try_into()?,
    )?
    .trim_end_matches(char::from(0))
    .to_string();
    offset += length_of_database_name as usize + 1;

    // 这里多减1是因为尾部的EOF
    let sql = String::from_utf8(buffer[offset..buffer.len() - 5].try_into()?)?.to_string();

    let event_body = EventBodyTypeCode2 {
        id_of_thread: id_of_thread,
        execute_time: execute_time,
        length_of_database_name: length_of_database_name,
        error_code: error_code,
        length_of_status_variable_block: length_of_status_variable_block,
        status_variables: status_variables,
        status_variables_string_vec_for_human: status_variables_string_vec_for_human,
        database_name: database_name,
        sql: sql,
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_161(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let log_filename_length = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
    offset += 4;

    let log_filename =
        String::from_utf8(buffer[offset..offset + log_filename_length as usize].try_into()?)?
            .trim_end_matches(char::from(0))
            .to_string();

    let event_body = EventBodyTypeCode161 {
        log_filename_length: log_filename_length,
        log_filename: log_filename,
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_162(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let mariadb_flags = vec![
        ("FL_STANDALONE", 1),
        ("FL_GROUP_COMMIT_ID", 2),
        ("FL_TRANSACTIONAL", 4),
        ("FL_ALLOW_PARALLEL", 8),
        ("FL_WAITED", 16),
        ("FL_DDL", 32),
        ("FL_PREPARED_XA", 64),
        ("FL_COMPLETED_XA", 128),
    ];

    let mariadb_flags_mapping = mariadb_flags.into_iter().collect::<HashMap<&str, u8>>();

    let gtid_sequence = u64::from_le_bytes(buffer[offset..offset + 8].try_into()?);
    offset += 8;

    let replication_domain_id = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
    offset += 4;

    let flags = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let mut event_body = EventBodyTypeCode162 {
        gtid_sequence: gtid_sequence,
        replication_domain_id: replication_domain_id,
        flags: flags,
        commit_id: None,
        format_id: None,
        gtid_length: None,
        bqual_length: None,
        xid: None,
    };

    if flags & mariadb_flags_mapping.get("FL_GROUP_COMMIT_ID").unwrap() > 0 {
        let commit_id = u64::from_le_bytes(buffer[offset..offset + 8].try_into()?);
        event_body.commit_id = Some(commit_id);
    } else if flags
        & (mariadb_flags_mapping.get("FL_PREPARED_XA").unwrap()
            | mariadb_flags_mapping.get("FL_COMPLETED_XA").unwrap())
        > 0
    {
        let format_id = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
        event_body.format_id = Some(format_id);
        offset += 4;

        let gtid_length = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
        event_body.gtid_length = Some(gtid_length);
        offset += 1;

        let bqual_length = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
        event_body.bqual_length = Some(bqual_length);
        offset += 1;

        let xid = buffer[offset..offset + gtid_length as usize + bqual_length as usize].to_vec();
        event_body.xid = Some(xid);
    }

    Ok(Box::new(event_body))
}

pub fn deal_type_code_5(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let data_type = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let value = u64::from_le_bytes(buffer[offset..offset + 8].try_into()?);

    let event_body = EventBodyTypeCode5 {
        data_type: data_type,
        value: value,
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_4(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let position_of_the_first_event_in_next_log_file =
        u64::from_le_bytes(buffer[offset..offset + 8].try_into()?);
    offset += 8;

    let file_name_of_next_binary_log =
        String::from_utf8(buffer[offset..buffer.len() - 4].try_into()?)?
            .trim_end_matches(char::from(0))
            .to_string();

    let event_body = EventBodyTypeCode4 {
        position_of_the_first_event_in_next_log_file: position_of_the_first_event_in_next_log_file,
        file_name_of_next_binary_log: file_name_of_next_binary_log,
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_23_to_25(
    mut buffer: Vec<u8>,
    type_code: u8,
    table_structs: &HashMap<u64, EventBodyTypeCode19>,
) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let mariadb_flags = vec![
        (0x0001_u16, "End of statement"),
        (0x0002, "No foreign key checks"),
        (0x0004, "No unique key checks"),
        (0x0008, "Indicates that rows in this event are complete"),
        (0x0010, "No check constraints"),
    ];

    let type_string_for_human = match type_code {
        23 => "insert",
        24 => "update",
        25 => "delete",
        _ => "unknown",
    }
    .to_string();

    // table id part
    let mut table_id_vec = buffer[offset..offset + 6].to_vec();
    table_id_vec.splice(table_id_vec.len()..table_id_vec.len(), vec![0, 0]);
    let table_id = u64::from_le_bytes(table_id_vec.as_slice().try_into()?);
    offset += 6;

    // flags part
    let flags = u16::from_le_bytes(buffer[offset..offset + 2].try_into()?);
    offset += 2;

    // flags for human part
    let mut flags_string_for_human = Vec::new();
    for mariadb_flag in mariadb_flags {
        if mariadb_flag.0 & flags > 0 {
            flags_string_for_human.push(mariadb_flag.1);
        }
    }

    // number of columns part
    let (number_of_columns, skip) = parse_lenenc(&buffer[offset..])?;

    offset += skip as usize;

    // columns used part
    let columns_used_n_byte = (number_of_columns + 7) / 8;
    let columns_used = parse_bitmap(
        &buffer[offset..offset + columns_used_n_byte as usize],
        number_of_columns,
    );

    offset += columns_used_n_byte as usize;

    // columns used for update part
    let mut columns_used_for_update = None;
    if type_code == 24 {
        let columns_used_for_update_n_byte = (number_of_columns + 7) / 8;

        let result = parse_bitmap(
            &buffer[offset..offset + columns_used_for_update_n_byte as usize],
            number_of_columns,
        );

        columns_used_for_update = Some(result);

        offset += columns_used_for_update_n_byte as usize;
    }

    // null bitmap part
    let null_bitmap_n_byte = (number_of_columns + 7) / 8;
    let null_bitmap = parse_bitmap(
        &buffer[offset..offset + null_bitmap_n_byte as usize],
        number_of_columns,
    );
    offset += null_bitmap_n_byte as usize;

    // get table info
    let table_info = table_structs.get(&table_id).unwrap();

    // column data part
    let (column_data_vec, skip) =
        parse_column_data_for_row_event(&mut buffer[offset..], &table_info, &null_bitmap)?;

    offset += skip;

    // create a basic event body
    let mut event_body = EventBodyTypeCode23To25 {
        type_string_for_human: type_string_for_human,
        table_id: table_id,
        flags: flags,
        number_of_columns: number_of_columns,
        columns_used: columns_used,
        columns_used_for_update: columns_used_for_update,
        null_bitmap: null_bitmap,
        column_data: column_data_vec,
        null_bitmap_for_update: None,
        column_data_for_update: None,
    };

    // if this is a update record
    if type_code == 24 {
        // null bitmap for update part
        let null_bitmap_for_update_n_byte = (number_of_columns + 7) / 8;
        let null_bitmap_for_update = parse_bitmap(
            &mut buffer[offset..offset + null_bitmap_for_update_n_byte as usize],
            number_of_columns,
        );
        offset += null_bitmap_for_update_n_byte as usize;

        // column data for update part
        let (column_data_for_update_vec, _skip) = parse_column_data_for_row_event(
            &mut buffer[offset..],
            &table_info,
            &null_bitmap_for_update,
        )?;

        event_body.null_bitmap_for_update = Some(null_bitmap_for_update);
        event_body.column_data_for_update = Some(column_data_for_update_vec);

        // offset += skip;
    }

    Ok(Box::new(event_body))
}

pub fn deal_type_code_13(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let event_body = EventBodyTypeCode13 {
        first_seed: u64::from_le_bytes(buffer[0..8].try_into()?),
        second_seed: u64::from_le_bytes(buffer[8..16].try_into()?),
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_164(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let event_body = EventBodyTypeCode164 {
        encryption_scheme: u8::from_le_bytes(buffer[0..1].try_into()?),
        encryption_key_version: u32::from_le_bytes(buffer[1..5].try_into()?),
        nonce: buffer[5..17].to_vec(),
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_38(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let length_of_gtrid = u32::from_le_bytes(buffer[5..9].try_into()?);
    let length_of_bqual = u8::from_le_bytes(buffer[9..10].try_into()?);

    let event_body = EventBodyTypeCode38 {
        one_phase_commit: u8::from_le_bytes(buffer[0..1].try_into()?),
        format_id: u32::from_le_bytes(buffer[1..5].try_into()?),
        length_of_gtrid: length_of_gtrid,
        length_of_bqual: length_of_bqual,
        xid: buffer[10..10 + length_of_gtrid as usize + length_of_bqual as usize].to_vec(),
    };

    Ok(Box::new(event_body))
}

pub fn deal_type_code_14(buffer: Vec<u8>) -> Result<Box<dyn EventBody>, BoxedError> {
    let mut offset = 0;

    let length_of_user_variable_name = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);
    offset += 4;

    let name_of_user_variable = String::from_utf8(
        buffer[offset..offset + length_of_user_variable_name as usize].try_into()?,
    )?;
    offset += length_of_user_variable_name as usize;

    let null_indicator = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let mut event_body = EventBodyTypeCode14 {
        length_of_user_variable_name: length_of_user_variable_name,
        name_of_user_variable: name_of_user_variable,
        null_indicator: null_indicator,
        variable_type: None,
        variable_type_string_for_human: None,
        collation_number: None,
        length_of_value: None,
        value: None,
        flags: None,
    };

    if null_indicator > 0 {
        let variable_type = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
        offset+=1;
        event_body.variable_type = Some(variable_type);

        let variable_type_mapping = vec![
            (0,"STRING_RESULT"),
            (1,"REAL_RESULT"),
            (2,"INT_RESULT"),
            (3,"ROW_RESULT"),
            (4,"DECIMAL_RESULT")
        ].iter()
        .map(|v| {
            (v.0, v.1.to_string())
        })
        .collect::<HashMap<u8, String>>();
        event_body.variable_type_string_for_human = Some(variable_type_mapping.get(&variable_type).unwrap().to_owned());

        let collation_number = u32::from_le_bytes(buffer[offset..offset+4].try_into()?);
        offset+=4;
        event_body.collation_number = Some(collation_number);

        let length_of_value = u32::from_le_bytes(buffer[offset..offset+4].try_into()?);
        offset+=4;
        event_body.length_of_value=Some(length_of_value);

        let value = String::from_utf8(buffer[offset..offset+length_of_value as usize].try_into()?)?;
        offset += length_of_value as usize;
        event_body.value = Some(value);

        let flags = u8::from_le_bytes(buffer[offset..offset+1].try_into()?);
        event_body.flags = Some(flags);
    }

    Ok(Box::new(event_body))
}

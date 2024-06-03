use std::{
    collections::HashMap,
    env,
};

use mariadb_binlog_parse::model::EventBodyTypeCode19;
use mariadb_binlog_parse::service::*;
use mariadb_binlog_parse::util::{check_file_magic_number, get_file};

const EVENT_HEADER_LENGTH: usize = 19;

type BoxedError = Box<dyn std::error::Error>;

fn main() -> Result<(), BoxedError> {
    let args = env::args().collect::<Vec<String>>();

    if args.len() < 2 {
        panic!("have no enough arguments. please input the binlog file path");
    }

    let binlog_file_path = args[1].clone();

    // cargo run --bin mariadb_binlog_parse --features="test"
    // 上述指令用于进行测试，即运行下面if中的代码块
    // 用于测试单条日志
    if cfg!(feature = "test") {
        let mut offset = 75227;

        let mut file = get_file(&binlog_file_path)?;

        let mut table_structs: HashMap<u64, EventBodyTypeCode19> = HashMap::new();

        let event_body = EventBodyTypeCode19 {
            table_id: 230,
            reserved_for_future_use: 1,
            database_name_length: 8,
            database_name: "test1223".to_string(),
            table_name_length: 2,
            table_name: "t4".to_string(),
            number_of_columns: 4,
            column_types: vec![
                3,
                252,
                252,
                15,
            ],
            column_types_string_for_human: vec![
                "MYSQL_TYPE_LONG".to_string(),
                "MYSQL_TYPE_BLOB".to_string(),
                "MYSQL_TYPE_BLOB".to_string(),
                "MYSQL_TYPE_VARCHAR".to_string(),
            ],
            number_of_metadata_block: 4,
            metadata_block: vec![
                2,
                2,
                16,
                39,
            ],
            metadata_block_string_for_human: vec![
                "field type id is: 252, field type name is: MYSQL_TYPE_BLOB, infomation is [field size is 2 bytes]".to_string(),
                "field type id is: 252, field type name is: MYSQL_TYPE_BLOB, infomation is [field size is 2 bytes]".to_string(),
                "field type id is: 15, field type name is: MYSQL_TYPE_VARCHAR, infomation is [the maximum length of the string is 10000 byte]".to_string(),
            ],
            metadata_block_data_raw: vec![
                vec![
                    2,
                ],
                vec![
                    2,
                ],
                vec![
                    16,
                    39,
                ],
            ],
            columns_can_be_null: vec![
                false,
                true,
                true,
                true,
            ],
            optional_metadata_block: vec![
                14,
                108,
                66,
                131,
                64,
            ],
        };

        table_structs
            .entry(event_body.table_id)
            .or_insert(event_body);

        let header = get_event_header(&mut file, offset)?;
        println!("{:#?}", header);
        offset += EVENT_HEADER_LENGTH as u64;

        let body = get_event_body(
            &mut file,
            offset,
            header.event_length,
            header.type_code,
            &mut table_structs,
        )?;
        println!("{:#?}", body);

        Ok(())
    } else {
        let mut offset: u64 = 0;

        let mut file = get_file(&binlog_file_path)?;

        let is_binlog_file = check_file_magic_number(&mut file)?;

        if !is_binlog_file {
            panic!("this is not a binglog file");
        }

        let file_length = (&file).metadata()?.len();

        offset += 4;

        let mut table_structs: HashMap<u64, EventBodyTypeCode19> = HashMap::new();

        loop {
            let header = get_event_header(&mut file, offset)?;
            println!("{:#?}", header);
            offset += EVENT_HEADER_LENGTH as u64;

            let body = get_event_body(
                &mut file,
                offset,
                header.event_length,
                header.type_code,
                &mut table_structs,
            )?;
            println!("{:#?}", body);
            offset = header.next_event_position as u64;

            println!();
            println!();

            if offset >= file_length {
                println!("It's the end of file");
                break;
            }
        }

        Ok(())
    }
}

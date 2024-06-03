use std::{collections::HashMap, fs::{File, OpenOptions}, io::Read};

use chrono::{DateTime, FixedOffset};
use lazy_static::lazy_static;

use base64::prelude::*;
use bitvec::prelude::*;

use crate::model::*;

type BoxedError = Box<dyn std::error::Error>;

lazy_static! {
    static ref FIXED_LENGTH_DATA_TYPE: HashMap<String, u8> = {
        let mut m = HashMap::new();
        m.insert("MYSQL_TYPE_NULL".to_string(), 0);
        m.insert("MYSQL_TYPE_TINY".to_string(), 1);
        m.insert("MYSQL_TYPE_YEAR".to_string(), 2);
        m.insert("MYSQL_TYPE_SHORT".to_string(), 2);
        m.insert("MYSQL_TYPE_INT24".to_string(), 3);
        m.insert("MYSQL_TYPE_LONG".to_string(), 4);
        m.insert("MYSQL_TYPE_LONGLONG".to_string(), 8);
        m.insert("MYSQL_TYPE_FLOAT".to_string(), 4);
        m.insert("MYSQL_TYPE_DOUBLE".to_string(), 8);
        m
    };
}

pub fn get_file(file_path: &str) -> Result<File, BoxedError> {
    let f = OpenOptions::new().read(true).open(file_path)?;

    Ok(f)
}

pub fn check_file_magic_number(file: &mut File) -> Result<bool, BoxedError> {
    let mut buffer = [0u8; 4];

    file.read_exact(&mut buffer)?;

    let hex_string = buffer
        .iter()
        .map(|v| format!("{:02X}", v))
        .collect::<String>()
        .to_lowercase();

    Ok(hex_string == "fe62696e")
}

pub fn parse_lenenc(buffer: &[u8]) -> Result<(u64, u8), BoxedError> {
    let first_byte = u8::from_le_bytes(buffer[0..1].try_into()?);

    if first_byte < 251 {
        return Ok((first_byte as u64, 1));
    } else if first_byte == 252 {
        let result = u16::from_le_bytes(buffer[1..3].try_into()?);
        return Ok((result as u64, 3));
    } else if first_byte == 253 {
        let mut temp_buffer = buffer[1..4].to_vec();
        temp_buffer.splice(temp_buffer.len()..temp_buffer.len(), vec![0]);
        let result = u32::from_le_bytes(temp_buffer.as_slice().try_into()?);
        return Ok((result as u64, 4));
    } else if first_byte == 254 {
        let result = u64::from_le_bytes(buffer[1..9].try_into()?);
        return Ok((result as u64, 9));
    } else {
        return Err(Box::new(MyError("lenenc parse error".to_string())));
    }
}

// 参考 https://github.com/mysql/mysql-server/blob/mysql-cluster-8.0.22/include/field_types.h#L52
pub fn get_field_types_mapping() -> Result<HashMap<u8, String>, BoxedError> {
    let mut f = OpenOptions::new().read(true).open("field_types.txt")?;

    let mut s = String::new();
    f.read_to_string(&mut s)?;

    // 这里定位u16是因为最大值是255
    // +1之后会越界
    let mut count: u16 = 0;
    let result = s
        .lines()
        .map(|line| {
            let data = line.split(",").nth(0).unwrap();
            let data_vec = data.split(" = ").collect::<Vec<&str>>();

            let return_data;

            if data_vec.len() == 1 {
                return_data = (count as u8, data_vec[0].to_string());
                count += 1;
            } else {
                let id = data_vec[1].parse::<u8>().unwrap();
                return_data = (id as u8, data_vec[0].to_string());
                count = id as u16 + 1;
            }

            return_data
        })
        .collect::<HashMap<u8, String>>();

    Ok(result)
}

// 参考 https://github.com/mysql/mysql-server/blob/mysql-cluster-8.0.22/libbinlogevents/include/rows_event.h#L192
pub fn get_metadata_block_mapping() -> Result<HashMap<u8, u8>, BoxedError> {
    let mut f = OpenOptions::new()
        .read(true)
        .open("metablock_mapping.txt")?;

    let mut s = String::new();

    f.read_to_string(&mut s)?;

    let result = s
        .lines()
        .map(|line| {
            let data_vec = line.split(",").collect::<Vec<&str>>();

            let column_type = data_vec[1].parse::<u8>().unwrap();
            let byte_quantity = data_vec[2].parse::<u8>().unwrap();

            (column_type, byte_quantity)
        })
        .collect::<HashMap<u8, u8>>();

    Ok(result)
}

pub fn parse_metadata_block(
    metadata_block_mapping: &HashMap<u8, u8>,
    field_types_mapping: &HashMap<u8, String>,
    metadata_block: &Vec<u8>,
    metadata_block_offset: u8,
    content_type: u8,
) -> Result<(String, Vec<u8>, u8), BoxedError> {
    let result;

    let metadata_block_length = metadata_block_mapping
        .get(&content_type)
        .or(Some(&0))
        .unwrap()
        .clone();

    let metadata_block_data = metadata_block
        [metadata_block_offset as usize..(metadata_block_offset + metadata_block_length) as usize]
        .to_vec();

    if metadata_block_length == 0 {
        result = Ok(("".to_string(), Vec::new(), 0));
    } else {
        let field_types_string_for_human = field_types_mapping.get(&content_type).unwrap();

        let infomation = match content_type {
            4 => {
                format!("the sizeof(float) is {}", metadata_block_data[0])
            }
            5 => {
                format!("the sizeof(dobule) is {}", metadata_block_data[0])
            }
            15 => {
                let mut metadata_block_data_clone = metadata_block_data.clone();
                metadata_block_data_clone.reverse();
                format!(
                    "the maximum length of the string is {} byte",
                    u16::from_be_bytes(metadata_block_data_clone.try_into().unwrap())
                )
            }
            16 => {
                format!("the length in bits of the bitfield is {}, the number of bytes occupied by the bitfield is {}", metadata_block_data[0], metadata_block_data[1])
            }
            17 => {
                format!(
                    "the number of decimals for the fractional part is {}",
                    metadata_block_data[0]
                )
            }
            18 => {
                format!(
                    "the number of decimals for the fractional part is {}",
                    metadata_block_data[0]
                )
            }
            19 => {
                format!(
                    "the number of decimals for the fractional part is {}",
                    metadata_block_data[0]
                )
            }
            246 => {
                format!(
                    "the length of precision is {}, the length of decimals is {}",
                    metadata_block_data[0], metadata_block_data[1]
                )
            }
            252 => {
                format!("field size is {} bytes", metadata_block_data[0])
            }
            253 => {
                let real_field_type_id = metadata_block_data[0];
                let real_field_type_name = field_types_mapping.get(&real_field_type_id).unwrap();
                let length = metadata_block_data[1];

                format!(
                    "real field type id is {}, real field type name is {}, storage length is {}",
                    real_field_type_id, real_field_type_name, length
                )
            }
            254 => {
                format!("field size is {} bytes", metadata_block_data[1])
            }
            255 => {
                format!(
                    "the number of bytes needed to represent the length of the geometry is {}",
                    metadata_block_data[0]
                )
            }
            _ => "".to_string(),
        };

        let content = format!(
            "field type id is: {}, field type name is: {}, infomation is [{}]",
            content_type, field_types_string_for_human, infomation
        );

        result = Ok((content, metadata_block_data, metadata_block_length));
    }

    result
}

pub fn parse_bitmap(buffer: &[u8], truncate: u64) -> Vec<bool> {
    let mut offset = 0;

    let mut result = Vec::new();

    for _ in 0..buffer.len() {
        let binary_string = format!("{:08b}", buffer[offset]);
        result.extend({
            let mut bool_vec = binary_string
                .split("")
                .filter(|v| !v.is_empty())
                .map(|v| match v {
                    "1" => true,
                    "0" => false,
                    _ => false,
                })
                .collect::<Vec<_>>();

            bool_vec.reverse();

            bool_vec
        });
        offset += 1;
    }
    result.truncate(truncate as usize);

    result
}

fn bin_to_decimal(
    buffer: &mut [u8],
    precision: usize,
    decimals: usize,
) -> Result<(String, usize), BoxedError> {
    // 获取这个数值的符号
    let _sign = buffer[0] & 0x80;
    let sign = match _sign {
        0 => -1,
        _ => 1,
    };

    // 计算需要占用多少字节
    let integer_part_length = precision - decimals;

    let integer_part_byte_n = parse_quantity_of_bytes_for_decimal_part(integer_part_length);
    let decimal_part_byte_n = parse_quantity_of_bytes_for_decimal_part(decimals);

    let total_byte_n = integer_part_byte_n + decimal_part_byte_n;

    // 如果是负数需要对所有的bit进行取反
    if sign < 0 {
        for i in 0..total_byte_n {
            buffer[i] = !buffer[i];
        }
    }

    // 将最高位取反
    buffer[0] ^= 0x80;

    let mut numberic_string = parse_numberic_for_decimal(&buffer[0..integer_part_byte_n])?;
    numberic_string.push_str(".");
    numberic_string.push_str(&parse_numberic_for_decimal(
        &buffer[integer_part_byte_n..total_byte_n],
    )?);

    Ok((numberic_string, total_byte_n))
}

/// 参数的n为decimal中整数或者小数占据多少位
/// 这里的位是指数学意义上的例如678就是占用3位
/// 这里的计算规则是每9位数字会占用4字节
/// 然后不满9位的话会按照一个映射占用相应的字节数量
/// 详情参考下方链接
/// https://github.com/google/mysql/blob/master/strings/decimal.c#L1096
fn parse_quantity_of_bytes_for_decimal_part(n: usize) -> usize {
    let quantity_for_9_digits = n / 9;
    let remaining_digits = n % 9;

    quantity_for_9_digits * 4 + (remaining_digits + 1) / 2
}

/// 将decimal的整数部分或者小数部分的bin转换成数字
fn parse_numberic_for_decimal(buffer: &[u8]) -> Result<String, BoxedError> {
    let buffer_length = buffer.len();

    let remainder = buffer_length % 4;

    let mut data = buffer.to_owned();

    let mut result = Vec::new();

    if remainder > 0 {
        let mut additonal_data = Vec::new();

        for _ in 0..4 - remainder {
            additonal_data.push(0);
        }

        data.splice(0..0, additonal_data);
    }

    for i in 0..data.len() / 4 {
        result.push(u32::from_be_bytes(data[i * 4..(i + 1) * 4].try_into()?).to_string());
    }

    Ok(result.join(""))
}

pub fn parse_column_data_for_row_event(
    buffer: &mut [u8],
    table_info: &EventBodyTypeCode19,
    null_bitmap: &Vec<bool>,
) -> Result<(Vec<String>, usize), BoxedError> {
    let mut offset = 0;

    let field_type_vec = table_info
        .column_types_string_for_human
        .iter()
        .map(|v| v.as_str())
        .collect::<Vec<&str>>();

    let mut metadata_block_raw_iter = table_info.metadata_block_data_raw.iter();

    let mut column_data_vec = Vec::new();

    let should_be_decode_from_table_event_data_type = vec![
        "MYSQL_TYPE_BIT",
        "MYSQL_TYPE_ENUM",
        "MYSQL_TYPE_SET",
        "MYSQL_TYPE_NEWDECIMAL",
        "MYSQL_TYPE_DECIMAL",
        "MYSQL_TYPE_VARCHAR",
        "MYSQL_TYPE_VAR_STRING",
        "MYSQL_TYPE_STRING",
        "MYSQL_TYPE_TINY_BLOB",
        "MYSQL_TYPE_MEDIUM_BLOB",
        "MYSQL_TYPE_LONG_BLOB",
        "MYSQL_TYPE_BLOB",
        "MYSQL_TYPE_TIMESTAMP2",
        "MYSQL_TYPE_DATETIME2",
        "MYSQL_TYPE_TIME2",
        "MYSQL_TYPE_FLOAT",
        "MYSQL_TYPE_DOUBLE",
    ];

    for i in 0..null_bitmap.len() {
        // 这里的做法不大优雅
        // 因为存在一种可能是，字段值有metadata但是他的null_bitmap是false
        // 那么在上述情况下如果将next的动作写在每个字段的处理块中就会发生遗漏next
        // 从而导致metadata和相应的处理块的错位
        // 但是如果不给metadata_block_data_raw进行初始化，编译器会报警
        // 综上所述采用了一个虚假的初始化
        let fake_data = Vec::new();
        let mut metadata_block_data_raw: Option<&Vec<u8>> = Some(&fake_data);
        if should_be_decode_from_table_event_data_type.contains(&field_type_vec[i]) {
            metadata_block_data_raw = metadata_block_raw_iter.next();
        }

        if !null_bitmap[i] {
            let field_length = FIXED_LENGTH_DATA_TYPE
                .get(field_type_vec[i])
                .unwrap_or(&0)
                .to_owned() as usize;

            let data = match field_type_vec[i] {
                "MYSQL_TYPE_TINY" => {
                    let result =
                        i8::from_le_bytes(buffer[offset..offset + field_length].try_into()?);
                    offset += field_length;
                    result.to_string()
                }
                "MYSQL_TYPE_SHORT" => {
                    let result =
                        i16::from_le_bytes(buffer[offset..offset + field_length].try_into()?);
                    offset += field_length;
                    result.to_string()
                }
                "MYSQL_TYPE_LONG" => {
                    let result =
                        i32::from_le_bytes(buffer[offset..offset + field_length].try_into()?);
                    offset += field_length;
                    result.to_string()
                }
                "MYSQL_TYPE_LONGLONG" => {
                    let result =
                        i64::from_le_bytes(buffer[offset..offset + field_length].try_into()?);
                    offset += field_length;
                    result.to_string()
                }
                "MYSQL_TYPE_FLOAT" => {
                    let result =
                        f32::from_le_bytes(buffer[offset..offset + field_length].try_into()?);
                    offset += field_length;
                    result.to_string()
                }
                "MYSQL_TYPE_DOUBLE" => {
                    let result =
                        f64::from_le_bytes(buffer[offset..offset + field_length].try_into()?);
                    offset += field_length;
                    result.to_string()
                }
                "MYSQL_TYPE_NEWDECIMAL" => {
                    let metadata_block_data = metadata_block_data_raw.unwrap();

                    let (numberic_string, skip) = bin_to_decimal(
                        &mut buffer[offset..],
                        metadata_block_data[0] as usize,
                        metadata_block_data[1] as usize,
                    )?;
                    offset += skip;
                    numberic_string
                }
                "MYSQL_TYPE_VARCHAR" => {
                    let metadata_block_data = metadata_block_data_raw.unwrap().clone();

                    let varchar_defined_length =
                        u16::from_le_bytes(metadata_block_data.try_into().unwrap());

                    let varchar_real_length: usize;

                    // 实际的varchar的长度获取是需要依赖19中的metadata的
                    // 如果定义的varchar长度超过255，那么再23~25的数据中使用2byte表示长度
                    // 如果定义的varchar长度小于等于255，那么在23~25的数据中使用1byte表示长度
                    if varchar_defined_length > 255 {
                        varchar_real_length =
                            u16::from_le_bytes(buffer[offset..offset + 2].try_into()?) as usize;
                        offset += 2;
                    } else {
                        varchar_real_length =
                            u8::from_le_bytes(buffer[offset..offset + 1].try_into()?) as usize;
                        offset += 1;
                    }

                    let result = try_convert_binary_to_string(&buffer[offset..offset+varchar_real_length]);

                    offset += varchar_real_length;

                    result
                }
                "MYSQL_TYPE_DATE" => {
                    let bits = buffer[offset..offset + 3].view_bits::<Lsb0>().to_bitvec();

                    let day = bits.get(0..5).unwrap().to_owned().load_le::<u8>();

                    let month = bits.get(5..9).unwrap().to_owned().load_le::<u8>();

                    let year = bits.get(9..).unwrap().to_owned().load_le::<u16>();

                    offset += 3;

                    format!("{}-{}-{}", year, month, day)
                }
                "MYSQL_TYPE_TIME2" => {
                    let bits = buffer[offset..offset + 3].view_bits::<Msb0>().to_bitvec();

                    let mut val: i32 = (bits.load_be::<u32>() - 0x800000) as i32;

                    if val < 0 {
                        val = -val;
                    }

                    let hour = (val >> 12) % (1 << 10);
                    let minute = (val >> 6) % (1 << 6);
                    let second = val % (1 << 6);

                    offset += 3;

                    format!("{:02}:{:02}:{02}", hour, minute, second)
                }
                "MYSQL_TYPE_DATETIME2" => {
                    let bits = buffer[offset..offset + 5].view_bits::<Msb0>().to_bitvec();

                    let val = bits.load_be::<u64>() - 0x8000000000;

                    let date_val = val >> 17;
                    let time_val = val % (1 << 17);

                    let day = date_val % (1 << 5);
                    let month = (date_val >> 5) % 13;
                    let year = (date_val >> 5) / 13;
                    let second = time_val % (1 << 6);
                    let minute = (time_val >> 6) % (1 << 6);
                    let hour = (time_val >> 12) % (1 << 12);

                    offset += 5;

                    format!(
                        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, minute, second
                    )
                }
                "MYSQL_TYPE_TIMESTAMP2" => {
                    let timestamp = u32::from_be_bytes(buffer[offset..offset + 4].try_into()?);

                    let datetime_utc = DateTime::from_timestamp(timestamp as i64, 0).unwrap();

                    let datetime_timezone =
                        datetime_utc.with_timezone(&FixedOffset::east_opt(8 * 3600).unwrap());

                    datetime_timezone.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                "MYSQL_TYPE_BLOB" => {
                    let blob_length_byte_n =
                        u8::from_le(metadata_block_data_raw.unwrap().clone()[0].try_into()?);

                    // println!("buffer is {:?}", blob_length_byte_n);

                    let blob_length = match blob_length_byte_n {
                        1 => {
                            let result = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);

                            offset += 1;

                            result as usize
                        }
                        2 => {
                            let result = u16::from_le_bytes(buffer[offset..offset + 2].try_into()?);

                            offset += 2;

                            result as usize
                        }
                        3 => {
                            let mut data = buffer[offset..offset + 3].to_vec();
                            // data.splice(0..0, [0]);
                            data.push(0);

                            let result = u32::from_le_bytes(data[..].try_into()?);

                            offset += 3;

                            result as usize
                        }
                        4 => {
                            let result = u32::from_le_bytes(buffer[offset..offset + 4].try_into()?);

                            offset += 4;

                            result as usize
                        }
                        _others => panic!("blob length by byte is only in range [1,4]"),
                    };

                    let result = try_convert_binary_to_string(&buffer[offset..offset + blob_length]);

                    offset += blob_length;

                    result
                }
                others => format!("type `{}` is not implement", others),
            };

            column_data_vec.push(data);
        }
    }

    Ok((column_data_vec, offset))
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Query__event.html#aff85b464cf52841608d74a5568a5c0f1
pub fn parse_status_variables(buffer: &Vec<u8>) -> Result<Vec<String>, BoxedError> {
    let length = buffer.len();

    let mut offset = 0;

    let mut results = Vec::new();

    loop {
        let code_id = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);

        offset += 1;

        let result = match code_id {
            0 => parse_status_variables_q_flag32_code(&buffer[offset..])?,
            1 => parse_status_variables_q_sql_mode_code(&buffer[offset..])?,
            3 => parse_status_variables_q_auto_increment(&buffer[offset..])?,
            4 => parse_status_variables_q_charset_code(&buffer[offset..])?,
            5 => parse_status_variables_q_timezone_code(&buffer[offset..])?,
            6 => parse_status_variables_q_catalog_nz_code(&buffer[offset..])?,
            7 => parse_status_variables_q_lc_time_names_code(&buffer[offset..])?,
            8 => parse_status_variables_q_charset_database_code(&buffer[offset..])?,
            9 => parse_status_variables_q_table_map_for_update_code(&buffer[offset..])?,
            11 => parse_status_variables_q_invoker(&buffer[offset..])?,
            128 => parse_status_variables_q_hrnow(&buffer[offset..])?,
            129 => parse_status_variables_q_xid(&buffer[offset..])?,
            others => {
                panic!(
                    "we found some unhandled status variables code is `{}`",
                    others
                );
            }
        };

        offset += result.1;
        results.push(result.0);

        if offset >= length {
            break;
        }
    }

    Ok(results)
}

fn parse_status_variables_q_flag32_code(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let bitmap = vec![
        (0x00004000, "OPTION_AUTO_IS_NULL"),
        (0x00080000, "OPTION_NOT_AUTOCOMMIT"),
        (0x04000000, "OPTION_NO_FOREIGN_KEY_CHECKS"),
        (0x08000000, "OPTION_RELAXED_UNIQUE_CHECKS"),
    ];

    let data = u32::from_le_bytes(buffer[0..4].try_into()?);

    let mut middle_result = Vec::new();
    for map in bitmap {
        if map.0 & data > 0 {
            middle_result.push(map.1.to_string());
        }
    }

    let result = format!("FLAGS2 is [{}]", middle_result.join(" | "));

    Ok((result, 4))
}

fn parse_status_variables_q_sql_mode_code(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let bitmap = vec![
        (0x00000001, "MODE_REAL_AS_FLOAT"),
        (0x00000002, "MODE_PIPES_AS_CONCAT"),
        (0x00000004, "MODE_ANSI_QUOTES"),
        (0x00000008, "MODE_IGNORE_SPACE"),
        (0x00000010, "MODE_NOT_USED"),
        (0x00000020, "MODE_ONLY_FULL_GROUP_BY"),
        (0x00000040, "MODE_NO_UNSIGNED_SUBTRACTION"),
        (0x00000080, "MODE_NO_DIR_IN_CREATE"),
        (0x00000100, "MODE_POSTGRESQL"),
        (0x00000200, "MODE_ORACLE"),
        (0x00000400, "MODE_MSSQL"),
        (0x00000800, "MODE_DB2"),
        (0x00001000, "MODE_MAXDB"),
        (0x00002000, "MODE_NO_KEY_OPTIONS"),
        (0x00004000, "MODE_NO_TABLE_OPTIONS"),
        (0x00008000, "MODE_NO_FIELD_OPTIONS"),
        (0x00010000, "MODE_MYSQL323"),
        (0x00020000, "MODE_MYSQL40"),
        (0x00040000, "MODE_ANSI"),
        (0x00080000, "MODE_NO_AUTO_VALUE_ON_ZERO"),
        (0x00100000, "MODE_NO_BACKSLASH_ESCAPES"),
        (0x00200000, "MODE_STRICT_TRANS_TABLES"),
        (0x00400000, "MODE_STRICT_ALL_TABLES"),
        (0x00800000, "MODE_NO_ZERO_IN_DATE"),
        (0x01000000, "MODE_NO_ZERO_DATE"),
        (0x02000000, "MODE_INVALID_DATES"),
        (0x04000000, "MODE_ERROR_FOR_DIVISION_BY_ZERO"),
        (0x08000000, "MODE_TRADITIONAL"),
        (0x10000000, "MODE_NO_AUTO_CREATE_USER"),
        (0x20000000, "MODE_HIGH_NOT_PRECEDENCE"),
        (0x40000000, "MODE_NO_ENGINE_SUBSTITUTION"),
        (0x80000000, "MODE_PAD_CHAR_TO_FULL_LENGTH"),
    ];

    let data = u64::from_le_bytes(buffer[0..8].try_into()?);

    let mut middle_result = Vec::new();
    for map in bitmap {
        if map.0 & data > 0 {
            middle_result.push(map.1.to_string());
        }
    }

    let result = format!("SQL_MODE is [{}]", middle_result.join(" | "));

    Ok((result, 8))
}

fn parse_status_variables_q_catalog_nz_code(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let length = u8::from_le_bytes(buffer[0..1].try_into()?);

    let catalog_name = String::from_utf8(buffer[1..1 + length as usize].try_into()?)?;

    let result = format!("catalog name is {}", catalog_name);

    Ok((result, length as usize + 1))
}

fn parse_status_variables_q_auto_increment(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let increment = u16::from_le_bytes(buffer[0..2].try_into()?);

    let offset = u16::from_le_bytes(buffer[2..4].try_into()?);

    let result = format!(
        "auto_increment increment is {}, auto increment offset is {}",
        increment, offset
    );

    Ok((result, 4))
}

fn parse_status_variables_q_charset_code(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let client_character_set = u16::from_le_bytes(buffer[0..2].try_into()?);

    let collation_connection = u16::from_le_bytes(buffer[2..4].try_into()?);

    let collation_server = u16::from_le_bytes(buffer[4..6].try_into()?);

    let result = format!("client character set is {}, collation connection is {}, collation server is {}, for detail please run query `SELECT id, character_set_name, collation_name FROM information_schema.COLLATIONS;`", client_character_set, collation_connection, collation_server);

    Ok((result, 6))
}

fn parse_status_variables_q_timezone_code(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let length = u8::from_le_bytes(buffer[0..1].try_into()?);

    let result = String::from_utf8(buffer[1..1 + length as usize].try_into()?)?;

    Ok((result, length as usize + 1))
}

fn parse_status_variables_q_lc_time_names_code(
    buffer: &[u8],
) -> Result<(String, usize), BoxedError> {
    let data = u16::from_le_bytes(buffer[0..2].try_into()?);

    let result = format!("lc time names code is {}", data);

    Ok((result, 2))
}

fn parse_status_variables_q_charset_database_code(
    buffer: &[u8],
) -> Result<(String, usize), BoxedError> {
    let data = u16::from_le_bytes(buffer[0..2].try_into()?);

    let result = format!("charset database code is {}", data);

    Ok((result, 2))
}

fn parse_status_variables_q_table_map_for_update_code(
    buffer: &[u8],
) -> Result<(String, usize), BoxedError> {
    let data = u8::from_le_bytes(buffer[0..1].try_into()?);

    let result = format!("table map for update code is {:08b}", data);

    Ok((result, 1))
}

fn parse_status_variables_q_invoker(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let mut offset = 0;
    let user_name_length = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let user_name =
        String::from_utf8(buffer[offset..offset + user_name_length as usize].try_into()?)?;
    offset += user_name_length as usize;

    let host_name_length = u8::from_le_bytes(buffer[offset..offset + 1].try_into()?);
    offset += 1;

    let host_name =
        String::from_utf8(buffer[offset..offset + host_name_length as usize].try_into()?)?;

    let result = format!("user name is {}, host name is {}", user_name, host_name);

    Ok((
        result,
        user_name_length as usize + 1 + host_name_length as usize + 1,
    ))
}

fn parse_status_variables_q_hrnow(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let mut raw_data = buffer[0..3].to_owned();
    raw_data.splice(raw_data.len()..raw_data.len(), [0]);

    let data = u32::from_le_bytes(raw_data[..].try_into()?);

    let result = format!("hrnow is {}", data);

    Ok((result, 3))
}

fn parse_status_variables_q_xid(buffer: &[u8]) -> Result<(String, usize), BoxedError> {
    let data = u64::from_le_bytes(buffer[0..8].try_into()?);

    let result = format!("xid is {}", data);

    Ok((result, 8))
}

fn try_convert_binary_to_string(buffer: &[u8]) -> String {
    let try_to_convert_to_string =
        String::from_utf8(buffer[..].try_into().unwrap());

    let result = match try_to_convert_to_string {
        Ok(s) => format!("this is a String, value is `{}`", s),
        Err(_e) => format!(
            "this is not a String, value with base64 is {}",
            BASE64_STANDARD.encode(buffer)
        ),
    };

    result
}

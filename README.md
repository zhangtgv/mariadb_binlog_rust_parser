# mariadb_binlog_rust_parser
a mariadb binlog parser written by rust

可以通过一下命令遍历binlog文件中的事件
cargo run --bin mariadb_binlog_parse -- /path/to/binlog/file

如果想要看特定条目的事件，可以使用如下命令
cargo run --bin mariadb_binlog_parse --features="test"
事件的offset需要在main.rs中调整

目前单机运行所遇到的事件已经实现，集群事件实现了部分，并且这部分也未进行测试。

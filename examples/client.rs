// =============================================================================
// examples/client.rs — 簡易 TCP client 範例
// =============================================================================
//
// 用法：
// - 先開另一個終端機啟動 server：
//   cargo run -- --server
// - 再執行：
//   cargo run --example client

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let addr = "127.0.0.1:6379";
    println!("Connecting to {} ...", addr);
    let mut stream = TcpStream::connect(addr)?;
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    let commands = [
        "set user:1 Alice",
        "set user:2 Bob",
        "get user:1",
        "list",
        "scan user:1 user:9",
        "stats",
        "delete user:2",
        "list",
    ];

    for cmd in commands {
        send_command(&mut stream, cmd)?;
        let resp = read_response(&mut reader)?;
        println!("> {}", cmd);
        println!("< {}", resp);
    }

    Ok(())
}

fn send_command(stream: &mut TcpStream, cmd: &str) -> std::io::Result<()> {
    stream.write_all(cmd.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn read_response(reader: &mut BufReader<TcpStream>) -> std::io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line.trim_end().to_string())
}

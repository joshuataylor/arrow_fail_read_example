use std::fs::File;
use arrow::ipc::reader::StreamReader;
use arrow::json::LineDelimitedWriter;
use arrow2::error::Result;
use arrow2::io::ipc::read::read_file_metadata;
use std::io::Write;

fn main() -> Result<()> {
    // Will read example_snowflake_data, then create "example_lines.txt" from this file to show it works
    arrow_example();

    // Will read example_snowflake_data, but will throw a "Arrow file does not contain correct header"
    arrow_example2();

    Ok(())
}

fn arrow_example() {
    let file = File::open("example_snowflake_data").unwrap();

    let reader = StreamReader::try_new(file, None).unwrap();
    let buf = Vec::new();
    let mut writer = LineDelimitedWriter::new(buf);

    reader.for_each(|batch| {
        let batch = batch.unwrap();
        writer.write_batches(&[batch]).unwrap();
    });
    writer.finish().unwrap();
    let buf = writer.into_inner();

    let bar = String::from_utf8(buf).unwrap();
    let mut file = File::create("example_lines.txt").unwrap();
    file.write_all(bar.as_ref()).unwrap();
}

fn arrow_example2() {
    let mut file = File::open("example_snowflake_data").unwrap();

    // Try and read metadata
    let _metadata = read_file_metadata(&mut file).unwrap();
}

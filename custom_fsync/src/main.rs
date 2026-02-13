use std::fs::File;
use std::path::Path;
use std::{fs, io};

fn recursive_fsync(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            recursive_fsync(&path)?;
            continue;
        }

        // note(ayush): skip everything which is not a regular file or directory because
        // they are not needed for the purpose of collecting outputs
        if !path.is_file() {
            continue;
        }

        match File::open(path) {
            Err(e) => {
                println!("error opening file: {}", e);
                continue;
            }
            Ok(fd) => {
                let _ = fd.sync_all();
            }
        }
    }

    Ok(())
}

fn main() -> io::Result<()> {
    recursive_fsync(Path::new("."))
}

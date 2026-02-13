use std::path::{Path, PathBuf};
use std::vec::Vec;
use std::{env, fs, io};

fn add_children_to_stack(dir: PathBuf, stack: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        match entry {
            Err(e) => {
                println!("error iterating over directory: {e}");
                continue;
            }
            Ok(res) => {
                stack.push(res.path());
            }
        }
    }

    Ok(())
}

fn fsync(top: PathBuf, verbose: bool) -> io::Result<()> {
    let mut stack: Vec<PathBuf> = Vec::new();

    if let Err(e) = add_children_to_stack(top, &mut stack) {
        println!("fatal: error reading current directory: {e}");
        return Err(e);
    }

    loop {
        if stack.is_empty() {
            break;
        }

        let next = stack.pop();
        if next.is_none() {
            break;
        }

        let path = next.unwrap();

        if verbose {
            println!("debug: syncing {}", path.display());
        }

        if path.is_dir() {
            if let Err(e) = add_children_to_stack(path, &mut stack) {
                println!("fatal: error reading child directory: {e}");
                return Err(e);
            }

            continue;
        }

        // note(ayush): skip everything which is not a regular file or directory because
        // they are not needed for the purpose of collecting outputs
        if !path.is_file() {
            if verbose {
                println!("debug: {} is not a regular file, skipping", path.display());
            }
            continue;
        }

        match fs::File::open(path) {
            Err(e) => {
                println!("warning: error opening file: {e}");
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
    let args: Vec<String> = env::args().collect();

    let mut verbose = false;
    if args.len() == 2 {
        if args[1] != "-v" && args[1] != "--verbose" {
            println!("warning: unrecognized argument `{}`, ignoring", args[1]);
        } else {
            verbose = true;
        }
    }

    fsync(Path::new(".").to_path_buf(), verbose)
}

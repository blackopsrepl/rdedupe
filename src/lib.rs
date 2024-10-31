//walks a filesystem and finds duplicate files
use indicatif::{ParallelProgressIterator, ProgressStyle};
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::error::Error;
use walkdir::WalkDir;

pub fn walk(path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            files.push(entry.path().to_str().unwrap().to_string());
        }
    }
    Ok(files)
}

//Find files matching a pattern
pub fn find(files: Vec<String>, pattern: &str) -> Vec<String> {
    let mut matches = Vec::new();
    for file in files {
        if file.contains(pattern) {
            matches.push(file);
        }
    }
    matches
}

/*  Parallel version of checksum using rayon with a mutex to ensure
 that the HashMap is not accessed by multiple threads at the same time
Uses indicatif to show a progress bar
*/
pub fn checksum(files: Vec<String>) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
    //set the progress bar style to allow for elapsed time and percentage complete
    let checksums = std::sync::Mutex::new(HashMap::new());
    let pb = indicatif::ProgressBar::new(files.len() as u64);
    let sty = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap();
    pb.set_style(sty);
    files.par_iter().progress_with(pb).for_each(|file| {
        let checksum = md5::compute(std::fs::read(file).unwrap());
        let checksum = format!("{:x}", checksum);
        let mut checksums = checksums.lock().unwrap();
        checksums
            .entry(checksum)
            .or_insert_with(Vec::new)
            .push(file.to_string());
    });
    Ok(checksums.into_inner().unwrap())
}

/*
Find all the files with more than one entry in the HashMap
*/
pub fn find_duplicates(checksums: HashMap<String, Vec<String>>) -> Vec<Vec<String>> {
    let mut duplicates = Vec::new();
    for (_checksum, files) in checksums {
        if files.len() > 1 {
            duplicates.push(files);
        }
    }
    duplicates
}

pub fn collect_statistics(files: Vec<String>, duplicates: Vec<String>) -> DataFrame {
    let file_sizes: Result<Vec<u64>, std::io::Error> = files
        .iter()
        .map(|file| std::fs::metadata(file).map(|meta| meta.len()))
        .collect();

    let file_sizes = file_sizes?;

    let df = std::sync::Mutex::new(DataFrame::new(vec![
        Series::new("File".into(), files),
        Series::new(
            "isDuplicate".into(),
            duplicates
                .iter()
                .map(|files| if files.len() > 1 { 1 } else { 0 })
                .collect::<Vec<i32>>(),
        ),
        Series::new("Size".into(), file_sizes),
        Series::new(
            "Occurrences".into(),
            duplicates
                .iter()
                .map(|files| files.len())
                .collect::<Vec<i32>>(),
        ),
        Series::new(
            "TotalSize".into(),
            duplicates
                .iter()
                .map(|files| {
                    files
                        .iter()
                        .map(|file| std::fs::metadata(file).unwrap().len())
                        .sum::<u64>()
                })
                .collect::<Vec<u64>>(),
        ),
        Series::new(
            "PotentialSave".into(),
            duplicates
                .iter()
                .map(|files| {
                    (files.len() - 1)
                        * files
                            .iter()
                            .map(|file| std::fs::metadata(file).unwrap().len() as i32)
                            .sum::<i32>()
                })
                .collect::<Vec<i32>>(),
        ),
    ])?);
    df
}

pub fn write_report(df: &std::sync::Mutex<DataFrame>) -> Result<(), Box<dyn Error>> {
    let mut guard = df.lock().unwrap();
    let mut file = std::fs::File::create("file_report.csv")?;
    CsvWriter::new(&mut file).finish(&mut guard)?;
    Ok(())
}

// invoke the actions along with the path and pattern and progress bar
pub fn run(path: &str, pattern: &str) -> Result<(), Box<dyn Error>> {
    let files = walk(path)?;
    let files = find(files, pattern);
    println!("Found {} files matching {}", files.len(), pattern);

    let checksums = checksum(files.clone())?;

    let duplicates = find_duplicates(checksums);

    let statistics = collect_statistics(files, duplicates);

    for duplicate in duplicates {
        println!("{:?}", duplicate);
    }
    println!("Found {} duplicate(s)", duplicates.len());

    write_report(statistics);

    Ok(())
}

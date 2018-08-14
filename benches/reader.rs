// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![feature(test)]
extern crate parquet;
extern crate test;

use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use test::Bencher;

#[bench]
fn record_reader_10k_collect(bench: &mut Bencher) {
  let path = Path::new("data/10k-v2.parquet");
  let file = File::open(&path).unwrap();
  let len = file.metadata().unwrap().len();
  let parquet_reader = SerializedFileReader::new(file).unwrap();

  bench.bytes = len;
  bench.iter(|| {
    let iter = parquet_reader.get_row_iter(None).unwrap();
    let _ = iter.collect::<Vec<_>>();
  })
}
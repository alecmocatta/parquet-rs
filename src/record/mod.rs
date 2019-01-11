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

//! Contains record-based API for reading Parquet files.

mod api;
pub mod reader;
pub mod schemas;
mod triplet;
pub mod types;

use column::reader::ColumnReader;
use errors::ParquetError;
use record::reader::Reader;
use schema::types::{ColumnDescPtr, ColumnPath, Type};
use std::{
  collections::HashMap,
  fmt::{self, Debug},
};
// pub use self::api::{List, ListAccessor, Map, MapAccessor, Row, RowAccessor};
// pub use self::triplet::TypedTripletIter;

pub trait DebugType {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error>;
}

pub trait Deserialize: Sized {
  type Schema: Debug + DebugType;
  type Reader: Reader<Item = Self>;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError>;
  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader;
}

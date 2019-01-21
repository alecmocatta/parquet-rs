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

use std::{
    collections::HashMap,
    fmt::{self, Display},
    marker::PhantomData,
};

use crate::{
    column::reader::ColumnReader,
    errors::ParquetError,
    record::reader::Reader,
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

pub trait DisplayType {
    fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error>;
}

struct DisplayDisplayType<T>(PhantomData<fn(T)>)
where
    T: DisplayType;
impl<T> DisplayDisplayType<T>
where
    T: DisplayType,
{
    fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> Display for DisplayDisplayType<T>
where
    T: DisplayType,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        T::fmt(f)
    }
}

pub trait Deserialize: Sized {
    type Schema: Display + DisplayType;
    type Reader: Reader<Item = Self>;

    /// Parse a [`Type`] into `Self::Schema`.
    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError>;

    /// Builds tree of readers for the specified schema recursively.
    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader;
}

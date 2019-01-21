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

//! Contains implementation of record assembly and converting Parquet types into
//! [`Row`](`::record::api::Row`)s.

use std::{collections::HashMap, convert::TryInto, error::Error, marker::PhantomData, rc::Rc};

use super::{
    triplet::TypedTripletIter,
    types::{Group, List, Map, Root, Timestamp, Value},
    Deserialize, DisplayDisplayType,
};
use crate::column::reader::ColumnReader;
use crate::data_type::{
    BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
    Int96, Int96Type,
};
use crate::errors::{ParquetError, Result};
use crate::file::reader::{FileReader, RowGroupReader};
use crate::schema::types::{ColumnDescPtr, ColumnPath, SchemaDescPtr, SchemaDescriptor, Type};

/// Default batch size for a reader
const DEFAULT_BATCH_SIZE: usize = 1024;

pub trait Reader {
    type Item;
    fn read(&mut self) -> Result<Self::Item>;
    fn advance_columns(&mut self) -> Result<()>;
    fn has_next(&self) -> bool;
    fn current_def_level(&self) -> i16;
    fn current_rep_level(&self) -> i16;
}

impl<A, B, C> Reader for sum::Sum3<A, B, C>
where
    A: Reader,
    B: Reader<Item = A::Item>,
    C: Reader<Item = A::Item>,
{
    type Item = A::Item;

    fn read(&mut self) -> Result<Self::Item> {
        match self {
            sum::Sum3::A(ref mut reader) => reader.read(),
            sum::Sum3::B(ref mut reader) => reader.read(),
            sum::Sum3::C(ref mut reader) => reader.read(),
        }
    }

    fn advance_columns(&mut self) -> Result<()> {
        match self {
            sum::Sum3::A(ref mut reader) => reader.advance_columns(),
            sum::Sum3::B(ref mut reader) => reader.advance_columns(),
            sum::Sum3::C(ref mut reader) => reader.advance_columns(),
        }
    }

    fn has_next(&self) -> bool {
        match self {
            sum::Sum3::A(ref reader) => reader.has_next(),
            sum::Sum3::B(ref reader) => reader.has_next(),
            sum::Sum3::C(ref reader) => reader.has_next(),
        }
    }

    fn current_def_level(&self) -> i16 {
        match self {
            sum::Sum3::A(ref reader) => reader.current_def_level(),
            sum::Sum3::B(ref reader) => reader.current_def_level(),
            sum::Sum3::C(ref reader) => reader.current_def_level(),
        }
    }

    fn current_rep_level(&self) -> i16 {
        match self {
            sum::Sum3::A(ref reader) => reader.current_rep_level(),
            sum::Sum3::B(ref reader) => reader.current_rep_level(),
            sum::Sum3::C(ref reader) => reader.current_rep_level(),
        }
    }
}

pub struct BoolReader {
    pub(super) column: TypedTripletIter<BoolType>,
}
impl Reader for BoolReader {
    type Item = bool;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read()
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct I32Reader {
    pub(super) column: TypedTripletIter<Int32Type>,
}
impl Reader for I32Reader {
    type Item = i32;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read()
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct I64Reader {
    pub(super) column: TypedTripletIter<Int64Type>,
}
impl Reader for I64Reader {
    type Item = i64;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read()
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct I96Reader {
    pub(super) column: TypedTripletIter<Int96Type>,
}
impl Reader for I96Reader {
    type Item = Int96;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read()
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct F32Reader {
    pub(super) column: TypedTripletIter<FloatType>,
}
impl Reader for F32Reader {
    type Item = f32;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read()
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct F64Reader {
    pub(super) column: TypedTripletIter<DoubleType>,
}
impl Reader for F64Reader {
    type Item = f64;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read()
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct ByteArrayReader {
    pub(super) column: TypedTripletIter<ByteArrayType>,
}
impl Reader for ByteArrayReader {
    type Item = Vec<u8>;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read().map(|data| data.data().to_owned())
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct FixedLenByteArrayReader {
    pub(super) column: TypedTripletIter<FixedLenByteArrayType>,
}
impl Reader for FixedLenByteArrayReader {
    type Item = Vec<u8>;

    fn read(&mut self) -> Result<Self::Item> {
        self.column.read().map(|data| data.data().to_owned())
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.column.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.column.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.column.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.column.current_rep_level()
    }
}

pub struct OptionReader<R> {
    pub(super) def_level: i16,
    pub(super) reader: R,
}
impl<R: Reader> Reader for OptionReader<R> {
    type Item = Option<R::Item>;

    fn read(&mut self) -> Result<Self::Item> {
        if self.reader.current_def_level() > self.def_level {
            self.reader.read().map(Some)
        } else {
            self.reader.advance_columns().map(|()| None)
        }
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.reader.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.reader.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.reader.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.reader.current_rep_level()
    }
}

pub struct RepeatedReader<R> {
    pub(super) def_level: i16,
    pub(super) rep_level: i16,
    pub(super) reader: R,
}
impl<R: Reader> Reader for RepeatedReader<R> {
    type Item = Vec<R::Item>;

    fn read(&mut self) -> Result<Self::Item> {
        let mut elements = Vec::new();
        loop {
            if self.reader.current_def_level() > self.def_level {
                elements.push(self.reader.read()?);
            } else {
                self.reader.advance_columns()?;
                // If the current definition level is equal to the definition level of this
                // repeated type, then the result is an empty list and the repetition level
                // will always be <= rl.
                break;
            }

            // This covers case when we are out of repetition levels and should close the
            // group, or there are no values left to buffer.
            if !self.reader.has_next() || self.reader.current_rep_level() <= self.rep_level {
                break;
            }
        }
        Ok(elements)
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.reader.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.reader.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.reader.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.reader.current_rep_level()
    }
}

pub struct KeyValueReader<K, V> {
    pub(super) def_level: i16,
    pub(super) rep_level: i16,
    pub(super) keys_reader: K,
    pub(super) values_reader: V,
}
impl<K: Reader, V: Reader> Reader for KeyValueReader<K, V> {
    type Item = Vec<(K::Item, V::Item)>;

    fn read(&mut self) -> Result<Self::Item> {
        let mut pairs = Vec::new();
        loop {
            if self.keys_reader.current_def_level() > self.def_level {
                pairs.push((self.keys_reader.read()?, self.values_reader.read()?));
            } else {
                self.keys_reader.advance_columns()?;
                self.values_reader.advance_columns()?;
                // If the current definition level is equal to the definition level of this
                // repeated type, then the result is an empty list and the repetition level
                // will always be <= rl.
                break;
            }

            // This covers case when we are out of repetition levels and should close the
            // group, or there are no values left to buffer.
            if !self.keys_reader.has_next()
                || self.keys_reader.current_rep_level() <= self.rep_level
            {
                break;
            }
        }

        Ok(pairs)
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.keys_reader.advance_columns()?;
        self.values_reader.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.keys_reader.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.keys_reader.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.keys_reader.current_rep_level()
    }
}

pub struct GroupReader {
    pub(super) def_level: i16,
    pub(super) readers: Vec<ValueReader>,
    pub(super) fields: Rc<HashMap<String, usize>>,
}
impl Reader for GroupReader {
    type Item = Group;

    fn read(&mut self) -> Result<Self::Item> {
        let mut fields = Vec::new();
        for reader in self.readers.iter_mut() {
            fields.push(reader.read()?);
        }
        Ok(Group(fields, self.fields.clone()))
    }

    fn advance_columns(&mut self) -> Result<()> {
        for reader in self.readers.iter_mut() {
            reader.advance_columns()?;
        }
        Ok(())
    }

    fn has_next(&self) -> bool {
        match self.readers.first() {
            Some(reader) => reader.has_next(),
            None => true,
        }
    }

    fn current_def_level(&self) -> i16 {
        match self.readers.first() {
            Some(reader) => reader.current_def_level(),
            None => panic!("Current definition level: empty group reader"),
        }
    }

    fn current_rep_level(&self) -> i16 {
        match self.readers.first() {
            Some(reader) => reader.current_rep_level(),
            None => panic!("Current repetition level: empty group reader"),
        }
    }
}

pub enum ValueReader {
    Bool(<bool as Deserialize>::Reader),
    U8(<u8 as Deserialize>::Reader),
    I8(<i8 as Deserialize>::Reader),
    U16(<u16 as Deserialize>::Reader),
    I16(<i16 as Deserialize>::Reader),
    U32(<u32 as Deserialize>::Reader),
    I32(<i32 as Deserialize>::Reader),
    U64(<u64 as Deserialize>::Reader),
    I64(<i64 as Deserialize>::Reader),
    F32(<f32 as Deserialize>::Reader),
    F64(<f64 as Deserialize>::Reader),
    Timestamp(<Timestamp as Deserialize>::Reader),
    Array(<Vec<u8> as Deserialize>::Reader),
    String(<String as Deserialize>::Reader),
    List(Box<<List<Value> as Deserialize>::Reader>),
    Map(Box<<Map<Value, Value> as Deserialize>::Reader>),
    Group(<Group as Deserialize>::Reader),
    Option(Box<<Option<Value> as Deserialize>::Reader>),
}
impl Reader for ValueReader {
    type Item = Value;

    fn read(&mut self) -> Result<Self::Item> {
        match self {
            ValueReader::Bool(ref mut reader) => reader.read().map(Value::Bool),
            ValueReader::U8(ref mut reader) => reader.read().map(Value::U8),
            ValueReader::I8(ref mut reader) => reader.read().map(Value::I8),
            ValueReader::U16(ref mut reader) => reader.read().map(Value::U16),
            ValueReader::I16(ref mut reader) => reader.read().map(Value::I16),
            ValueReader::U32(ref mut reader) => reader.read().map(Value::U32),
            ValueReader::I32(ref mut reader) => reader.read().map(Value::I32),
            ValueReader::U64(ref mut reader) => reader.read().map(Value::U64),
            ValueReader::I64(ref mut reader) => reader.read().map(Value::I64),
            ValueReader::F32(ref mut reader) => reader.read().map(Value::F32),
            ValueReader::F64(ref mut reader) => reader.read().map(Value::F64),
            ValueReader::Timestamp(ref mut reader) => reader.read().map(Value::Timestamp),
            ValueReader::Array(ref mut reader) => reader.read().map(Value::Array),
            ValueReader::String(ref mut reader) => reader.read().map(Value::String),
            ValueReader::List(ref mut reader) => reader.read().map(Value::List),
            ValueReader::Map(ref mut reader) => reader.read().map(Value::Map),
            ValueReader::Group(ref mut reader) => reader.read().map(Value::Group),
            ValueReader::Option(ref mut reader) => {
                reader.read().map(|x| Value::Option(Box::new(x)))
            }
        }
    }

    fn advance_columns(&mut self) -> Result<()> {
        match self {
            ValueReader::Bool(ref mut reader) => reader.advance_columns(),
            ValueReader::U8(ref mut reader) => reader.advance_columns(),
            ValueReader::I8(ref mut reader) => reader.advance_columns(),
            ValueReader::U16(ref mut reader) => reader.advance_columns(),
            ValueReader::I16(ref mut reader) => reader.advance_columns(),
            ValueReader::U32(ref mut reader) => reader.advance_columns(),
            ValueReader::I32(ref mut reader) => reader.advance_columns(),
            ValueReader::U64(ref mut reader) => reader.advance_columns(),
            ValueReader::I64(ref mut reader) => reader.advance_columns(),
            ValueReader::F32(ref mut reader) => reader.advance_columns(),
            ValueReader::F64(ref mut reader) => reader.advance_columns(),
            ValueReader::Timestamp(ref mut reader) => reader.advance_columns(),
            ValueReader::Array(ref mut reader) => reader.advance_columns(),
            ValueReader::String(ref mut reader) => reader.advance_columns(),
            ValueReader::List(ref mut reader) => reader.advance_columns(),
            ValueReader::Map(ref mut reader) => reader.advance_columns(),
            ValueReader::Group(ref mut reader) => reader.advance_columns(),
            ValueReader::Option(ref mut reader) => reader.advance_columns(),
        }
    }

    fn has_next(&self) -> bool {
        match self {
            ValueReader::Bool(ref reader) => reader.has_next(),
            ValueReader::U8(ref reader) => reader.has_next(),
            ValueReader::I8(ref reader) => reader.has_next(),
            ValueReader::U16(ref reader) => reader.has_next(),
            ValueReader::I16(ref reader) => reader.has_next(),
            ValueReader::U32(ref reader) => reader.has_next(),
            ValueReader::I32(ref reader) => reader.has_next(),
            ValueReader::U64(ref reader) => reader.has_next(),
            ValueReader::I64(ref reader) => reader.has_next(),
            ValueReader::F32(ref reader) => reader.has_next(),
            ValueReader::F64(ref reader) => reader.has_next(),
            ValueReader::Timestamp(ref reader) => reader.has_next(),
            ValueReader::Array(ref reader) => reader.has_next(),
            ValueReader::String(ref reader) => reader.has_next(),
            ValueReader::List(ref reader) => reader.has_next(),
            ValueReader::Map(ref reader) => reader.has_next(),
            ValueReader::Group(ref reader) => reader.has_next(),
            ValueReader::Option(ref reader) => reader.has_next(),
        }
    }

    fn current_def_level(&self) -> i16 {
        match self {
            ValueReader::Bool(ref reader) => reader.current_def_level(),
            ValueReader::U8(ref reader) => reader.current_def_level(),
            ValueReader::I8(ref reader) => reader.current_def_level(),
            ValueReader::U16(ref reader) => reader.current_def_level(),
            ValueReader::I16(ref reader) => reader.current_def_level(),
            ValueReader::U32(ref reader) => reader.current_def_level(),
            ValueReader::I32(ref reader) => reader.current_def_level(),
            ValueReader::U64(ref reader) => reader.current_def_level(),
            ValueReader::I64(ref reader) => reader.current_def_level(),
            ValueReader::F32(ref reader) => reader.current_def_level(),
            ValueReader::F64(ref reader) => reader.current_def_level(),
            ValueReader::Timestamp(ref reader) => reader.current_def_level(),
            ValueReader::Array(ref reader) => reader.current_def_level(),
            ValueReader::String(ref reader) => reader.current_def_level(),
            ValueReader::List(ref reader) => reader.current_def_level(),
            ValueReader::Map(ref reader) => reader.current_def_level(),
            ValueReader::Group(ref reader) => reader.current_def_level(),
            ValueReader::Option(ref reader) => reader.current_def_level(),
        }
    }

    fn current_rep_level(&self) -> i16 {
        match self {
            ValueReader::Bool(ref reader) => reader.current_rep_level(),
            ValueReader::U8(ref reader) => reader.current_rep_level(),
            ValueReader::I8(ref reader) => reader.current_rep_level(),
            ValueReader::U16(ref reader) => reader.current_rep_level(),
            ValueReader::I16(ref reader) => reader.current_rep_level(),
            ValueReader::U32(ref reader) => reader.current_rep_level(),
            ValueReader::I32(ref reader) => reader.current_rep_level(),
            ValueReader::U64(ref reader) => reader.current_rep_level(),
            ValueReader::I64(ref reader) => reader.current_rep_level(),
            ValueReader::F32(ref reader) => reader.current_rep_level(),
            ValueReader::F64(ref reader) => reader.current_rep_level(),
            ValueReader::Timestamp(ref reader) => reader.current_rep_level(),
            ValueReader::Array(ref reader) => reader.current_rep_level(),
            ValueReader::String(ref reader) => reader.current_rep_level(),
            ValueReader::List(ref reader) => reader.current_rep_level(),
            ValueReader::Map(ref reader) => reader.current_rep_level(),
            ValueReader::Group(ref reader) => reader.current_rep_level(),
            ValueReader::Option(ref reader) => reader.current_rep_level(),
        }
    }
}

pub struct RootReader<R>(pub R);
impl<R> Reader for RootReader<R>
where
    R: Reader,
{
    type Item = Root<R::Item>;

    fn read(&mut self) -> Result<Self::Item> {
        self.0.read().map(Root)
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.0.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.0.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.0.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.0.current_rep_level()
    }
}

pub struct TupleReader<T>(pub(super) T);

pub struct TryIntoReader<R: Reader, T>(pub(super) R, pub(super) PhantomData<fn(T)>);
impl<R: Reader, T> Reader for TryIntoReader<R, T>
where
    R::Item: TryInto<T>,
    <R::Item as TryInto<T>>::Error: Error,
{
    type Item = T;

    fn read(&mut self) -> Result<Self::Item> {
        self.0.read().and_then(|x| {
            x.try_into()
                .map_err(|err| ParquetError::General(err.description().to_owned()))
        })
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.0.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.0.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.0.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.0.current_rep_level()
    }
}

pub struct MapReader<R: Reader, F>(pub(super) R, pub(super) F);
impl<R: Reader, F, T> Reader for MapReader<R, F>
where
    F: FnMut(R::Item) -> Result<T>,
{
    type Item = T;

    fn read(&mut self) -> Result<Self::Item> {
        self.0.read().and_then(&mut self.1)
    }

    fn advance_columns(&mut self) -> Result<()> {
        self.0.advance_columns()
    }

    fn has_next(&self) -> bool {
        self.0.has_next()
    }

    fn current_def_level(&self) -> i16 {
        self.0.current_def_level()
    }

    fn current_rep_level(&self) -> i16 {
        self.0.current_rep_level()
    }
}

// ----------------------------------------------------------------------
// Row iterators

/// Iterator of rows. [`Row`](`super::types::Row`) can be used to read as untyped rows. A
/// tuple or a struct marked with `#[derive(ParquetDeserialize)]` can be used to read as
/// typed rows.
///
/// It is used either for a single row group to iterate over data in that row group, or
/// an entire file with auto buffering of all row groups.
pub struct RowIter<'a, R, T>
where
    R: FileReader,
    Root<T>: Deserialize,
{
    descr: SchemaDescPtr,
    // tree_builder: TreeBuilder,
    schema: <Root<T> as Deserialize>::Schema,
    file_reader: Option<&'a R>,
    current_row_group: usize,
    num_row_groups: usize,
    row_iter: Option<ReaderIter<T>>,
}

impl<'a, R, T> RowIter<'a, R, T>
where
    R: FileReader,
    Root<T>: Deserialize,
{
    /// Creates row iterator for all row groups in a file.
    pub fn from_file(proj: Option<Type>, reader: &'a R) -> Result<Self> {
        let descr =
            Self::get_proj_descr(proj, reader.metadata().file_metadata().schema_descr_ptr())?;
        let num_row_groups = reader.num_row_groups();

        let file_schema = reader.metadata().file_metadata().schema_descr_ptr();
        let file_schema = file_schema.root_schema();
        let schema = <Root<T> as Deserialize>::parse(file_schema)
            .map_err(|err| {
                // let schema: Type = <Root<T> as Deserialize>::render("", &<Root<T> as
                // Deserialize>::placeholder());
                let mut b = Vec::new();
                crate::schema::printer::print_schema(&mut b, file_schema);
                // let mut a = Vec::new();
                // print_schema(&mut a, &schema);
                ParquetError::General(format!(
                    "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
                    String::from_utf8(b).unwrap(),
                    // String::from_utf8(a).unwrap(),
                    DisplayDisplayType::<<Root<T> as Deserialize>::Schema>::new(),
                    err
                ))
            })?
            .1;

        Ok(Self {
            descr,
            schema,
            file_reader: Some(reader),
            current_row_group: 0,
            num_row_groups,
            row_iter: None,
        })
    }

    /// Creates row iterator for a specific row group.
    pub fn from_row_group(
        proj: Option<Type>,
        row_group_reader: &'a RowGroupReader,
    ) -> Result<Self> {
        let descr = Self::get_proj_descr(proj, row_group_reader.metadata().schema_descr_ptr())?;

        let file_schema = row_group_reader.metadata().schema_descr_ptr();
        let file_schema = file_schema.root_schema();
        let schema = <Root<T> as Deserialize>::parse(file_schema)
            .map_err(|err| {
                // let schema: Type = <Root<T> as Deserialize>::render("", &<Root<T> as
                // Deserialize>::placeholder());
                let mut b = Vec::new();
                crate::schema::printer::print_schema(&mut b, file_schema);
                // let mut a = Vec::new();
                // print_schema(&mut a, &schema);
                ParquetError::General(format!(
                    "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
                    String::from_utf8(b).unwrap(),
                    // String::from_utf8(a).unwrap(),
                    DisplayDisplayType::<<Root<T> as Deserialize>::Schema>::new(),
                    err
                ))
            })?
            .1;

        // Prepare lookup table of column path -> original column index
        // This allows to prune columns and map schema leaf nodes to the column readers
        let mut paths: HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)> = HashMap::new();
        let row_group_metadata = row_group_reader.metadata();
        for col_index in 0..row_group_reader.num_columns() {
            let col_meta = row_group_metadata.column(col_index);
            let col_path = col_meta.column_path().clone();
            // println!("path: {:?}", col_path);
            let col_descr = row_group_reader
                .metadata()
                .column(col_index)
                .column_descr_ptr();
            let col_reader = row_group_reader.get_column_reader(col_index).unwrap();

            let x = paths.insert(col_path, (col_descr, col_reader));
            assert!(x.is_none());
        }

        // Build reader for the message type, requires definition level 0
        let mut path = Vec::new();
        let reader = <Root<T>>::reader(&schema, &mut path, 0, 0, &mut paths, DEFAULT_BATCH_SIZE);
        let row_iter = ReaderIter::new(reader, row_group_reader.metadata().num_rows() as u64);

        // For row group we need to set `current_row_group` >= `num_row_groups`, because we
        // only have one row group and can't buffer more.
        Ok(Self {
            descr,
            schema,
            file_reader: None,
            current_row_group: 0,
            num_row_groups: 0,
            row_iter: Some(row_iter),
        })
    }

    /// Helper method to get schema descriptor for projected schema.
    /// If projection is None, then full schema is returned.
    #[inline]
    fn get_proj_descr(proj: Option<Type>, root_descr: SchemaDescPtr) -> Result<SchemaDescPtr> {
        match proj {
            Some(projection) => {
                // check if projection is part of file schema
                let root_schema = root_descr.root_schema();
                if !root_schema.check_contains(&projection) {
                    return Err(general_err!("Root schema does not contain projection"));
                }
                Ok(Rc::new(SchemaDescriptor::new(Rc::new(projection))))
            }
            None => Ok(root_descr),
        }
    }
}

impl<'a, R, T> Iterator for RowIter<'a, R, T>
where
    R: FileReader,
    Root<T>: Deserialize,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let mut row = None;
        if let Some(ref mut iter) = self.row_iter {
            row = iter.next();
        }

        while row.is_none() && self.current_row_group < self.num_row_groups {
            // We do not expect any failures when accessing a row group, and file reader
            // must be set for selecting next row group.
            let row_group_reader = &*self
                .file_reader
                .as_ref()
                .expect("File reader is required to advance row group")
                .get_row_group(self.current_row_group)
                .unwrap();
            self.current_row_group += 1;

            let mut paths: HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)> = HashMap::new();
            let row_group_metadata = row_group_reader.metadata();

            for col_index in 0..row_group_reader.num_columns() {
                let col_meta = row_group_metadata.column(col_index);
                let col_path = col_meta.column_path().clone();
                // println!("path: {:?}", col_path);
                let col_descr = row_group_reader
                    .metadata()
                    .column(col_index)
                    .column_descr_ptr();
                let col_reader = row_group_reader.get_column_reader(col_index).unwrap();

                let x = paths.insert(col_path, (col_descr, col_reader));
                assert!(x.is_none());
            }

            let mut path = Vec::new();
            let reader = <Root<T>>::reader(
                &self.schema,
                &mut path,
                0,
                0,
                &mut paths,
                DEFAULT_BATCH_SIZE,
            );
            let mut row_iter =
                ReaderIter::new(reader, row_group_reader.metadata().num_rows() as u64);

            row = row_iter.next();
            self.row_iter = Some(row_iter);
        }

        row
    }
}

/// Internal row iterator for a reader.
struct ReaderIter<T>
where
    Root<T>: Deserialize,
{
    root_reader: <Root<T> as Deserialize>::Reader,
    records_left: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T> ReaderIter<T>
where
    Root<T>: Deserialize,
{
    fn new(mut root_reader: <Root<T> as Deserialize>::Reader, num_records: u64) -> Self {
        // Prepare root reader by advancing all column vectors
        root_reader.advance_columns().unwrap();
        Self {
            root_reader,
            records_left: num_records,
            marker: PhantomData,
        }
    }
}

impl<T> Iterator for ReaderIter<T>
where
    Root<T>: Deserialize,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.records_left > 0 {
            self.records_left -= 1;
            Some(self.root_reader.read().unwrap().0)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::errors::Result;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::record::types::{Row, Value};
    use crate::schema::parser::parse_message_type;
    use crate::util::test_common::get_test_file;

    // Convenient macros to assemble row, list, map, and group.

    macro_rules! group {
        ( $( ($name:expr, $e:expr) ), * ) => {
            {
                #[allow(unused_mut)]
                let mut result = Vec::new();
                #[allow(unused_mut)]
                let mut keys = std::collections::HashMap::new();
                $(
                    keys.insert($name, result.len());
                    result.push($e);
                )*
                Group(result, std::rc::Rc::new(keys))
            }
        }
    }
    macro_rules! groupv {
        ( $( ($name:expr, $e:expr) ), * ) => {
            Value::Group(group!($( ($name, $e) ), *))
        }
    }
    macro_rules! row {
        ( $( ($name:expr, $e:expr) ), * ) => {
            group!($(($name,$e)),*)
        }
    }

    macro_rules! list {
        ( $( $e:expr ), * ) => {
            {
                #[allow(unused_mut)]
                let mut result = Vec::new();
                $(
                    result.push($e);
                )*
                List(result)
            }
        }
    }
    macro_rules! listv {
        ( $( $e:expr ), * ) => {
            Value::List(list!($($e),*))
        }
    }

    macro_rules! map {
        ( $( ($k:expr, $v:expr) ), * ) => {
            {
                #[allow(unused_mut)]
                let mut result = HashMap::new();
                $(
                    result.insert($k, $v);
                )*
                Map(result)
            }
        }
    }
    macro_rules! mapv {
        ( $( ($k:expr, $v:expr) ), * ) => {
            Value::Map(map!($(($k,$v)),*))
        }
    }

    macro_rules! somev {
        ( $e:expr ) => {
            Value::Option(Box::new(Some($e)))
        };
    }
    macro_rules! nonev {
        ( ) => {
            Value::Option(Box::new(None))
        };
    }

    #[test]
    fn test_file_reader_rows_nulls() {
        let rows = test_file_reader_rows::<Row>("nulls.snappy.parquet", None).unwrap();

        let expected_rows = vec![
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
            row![(
                "b_struct".to_string(),
                somev![groupv![("b_c_int".to_string(), nonev![])]]
            )],
        ];

        assert_eq!(rows, expected_rows);
    }

    #[test]
    fn test_file_reader_rows_nulls_typed() {
        type RowTyped = (Option<(Option<i32>,)>,);

        let rows = test_file_reader_rows::<RowTyped>("nulls.snappy.parquet", None).unwrap();

        let expected_rows: Vec<RowTyped> = vec![
            (Some((None,)),),
            (Some((None,)),),
            (Some((None,)),),
            (Some((None,)),),
            (Some((None,)),),
            (Some((None,)),),
            (Some((None,)),),
            (Some((None,)),),
        ];

        assert_eq!(rows, expected_rows);
    }

    #[test]
    fn test_file_reader_rows_nonnullable() {
        let rows = test_file_reader_rows::<Row>("nonnullable.impala.parquet", None).unwrap();

        let expected_rows = vec![row![
            ("ID".to_string(), Value::I64(8)),
            ("Int_Array".to_string(), listv![Value::I32(-1)]),
            (
                "int_array_array".to_string(),
                listv![listv![Value::I32(-1), Value::I32(-2)], listv![]]
            ),
            (
                "Int_Map".to_string(),
                mapv![(Value::String("k1".to_string()), Value::I32(-1))]
            ),
            (
                "int_map_array".to_string(),
                listv![
                    mapv![],
                    mapv![(Value::String("k1".to_string()), Value::I32(1))],
                    mapv![],
                    mapv![]
                ]
            ),
            (
                "nested_Struct".to_string(),
                groupv![
                    ("a".to_string(), Value::I32(-1)),
                    ("B".to_string(), listv![Value::I32(-1)]),
                    (
                        "c".to_string(),
                        groupv![(
                            "D".to_string(),
                            listv![listv![groupv![
                                ("e".to_string(), Value::I32(-1)),
                                ("f".to_string(), Value::String("nonnullable".to_string()))
                            ]]]
                        )]
                    ),
                    ("G".to_string(), mapv![])
                ]
            )
        ]];

        assert_eq!(rows, expected_rows);
    }

    #[test]
    fn test_file_reader_rows_nonnullable_typed() {
        type RowTyped = (
            i64,
            List<i32>,
            List<List<i32>>,
            Map<String, i32>,
            List<Map<String, i32>>,
            (
                i32,
                List<i32>,
                (List<List<(i32, String)>>,),
                Map<String, ((List<f64>,),)>,
            ),
        );

        let rows = test_file_reader_rows::<RowTyped>("nonnullable.impala.parquet", None).unwrap();

        let expected_rows: Vec<RowTyped> = vec![(
            8,
            list![-1],
            list![list![-1, -2], list![]],
            map![("k1".to_string(), -1)],
            list![map![], map![("k1".to_string(), 1i32)], map![], map![]],
            (
                -1,
                list![-1],
                (list![list![(-1, "nonnullable".to_string())]],),
                map![],
            ),
        )];

        assert_eq!(rows, expected_rows);
    }

    #[test]
    fn test_file_reader_rows_nullable() {
        let rows = test_file_reader_rows::<Row>("nullable.impala.parquet", None).unwrap();

        let expected_rows = vec![
            row![
                ("id".to_string(), somev![Value::I64(1)]),
                (
                    "int_array".to_string(),
                    somev![listv![
                        somev![Value::I32(1)],
                        somev![Value::I32(2)],
                        somev![Value::I32(3)]
                    ]]
                ),
                (
                    "int_array_Array".to_string(),
                    somev![listv![
                        somev![listv![somev![Value::I32(1)], somev![Value::I32(2)]]],
                        somev![listv![somev![Value::I32(3)], somev![Value::I32(4)]]]
                    ]]
                ),
                (
                    "int_map".to_string(),
                    somev![mapv![
                        (Value::String("k1".to_string()), somev![Value::I32(1)]),
                        (Value::String("k2".to_string()), somev![Value::I32(100)])
                    ]]
                ),
                (
                    "int_Map_Array".to_string(),
                    somev![listv![somev![mapv![(
                        Value::String("k1".to_string()),
                        somev![Value::I32(1)]
                    )]]]]
                ),
                (
                    "nested_struct".to_string(),
                    somev![groupv![
                        ("A".to_string(), somev![Value::I32(1)]),
                        ("b".to_string(), somev![listv![somev![Value::I32(1)]]]),
                        (
                            "C".to_string(),
                            somev![groupv![(
                                "d".to_string(),
                                somev![listv![
                                    somev![listv![
                                        somev![groupv![
                                            ("E".to_string(), somev![Value::I32(10)]),
                                            (
                                                "F".to_string(),
                                                somev![Value::String("aaa".to_string())]
                                            )
                                        ]],
                                        somev![groupv![
                                            ("E".to_string(), somev![Value::I32(-10)]),
                                            (
                                                "F".to_string(),
                                                somev![Value::String("bbb".to_string())]
                                            )
                                        ]]
                                    ]],
                                    somev![listv![somev![groupv![
                                        ("E".to_string(), somev![Value::I32(11)]),
                                        ("F".to_string(), somev![Value::String("c".to_string())])
                                    ]]]]
                                ]]
                            )]]
                        ),
                        (
                            "g".to_string(),
                            somev![mapv![(
                                Value::String("foo".to_string()),
                                somev![groupv![(
                                    "H".to_string(),
                                    somev![groupv![(
                                        "i".to_string(),
                                        somev![listv![somev![Value::F64(1.1)]]]
                                    )]]
                                )]]
                            )]]
                        )
                    ]]
                )
            ],
            row![
                ("id".to_string(), somev![Value::I64(2)]),
                (
                    "int_array".to_string(),
                    somev![listv![
                        nonev![],
                        somev![Value::I32(1)],
                        somev![Value::I32(2)],
                        nonev![],
                        somev![Value::I32(3)],
                        nonev![]
                    ]]
                ),
                (
                    "int_array_Array".to_string(),
                    somev![listv![
                        somev![listv![
                            nonev![],
                            somev![Value::I32(1)],
                            somev![Value::I32(2)],
                            nonev![]
                        ]],
                        somev![listv![
                            somev![Value::I32(3)],
                            nonev![],
                            somev![Value::I32(4)]
                        ]],
                        somev![listv![]],
                        nonev![]
                    ]]
                ),
                (
                    "int_map".to_string(),
                    somev![mapv![
                        (Value::String("k1".to_string()), somev![Value::I32(2)]),
                        (Value::String("k2".to_string()), nonev![])
                    ]]
                ),
                (
                    "int_Map_Array".to_string(),
                    somev![listv![
                        somev![mapv![
                            (Value::String("k3".to_string()), nonev![]),
                            (Value::String("k1".to_string()), somev![Value::I32(1)])
                        ]],
                        nonev![],
                        somev![mapv![]]
                    ]]
                ),
                (
                    "nested_struct".to_string(),
                    somev![groupv![
                        ("A".to_string(), nonev![]),
                        ("b".to_string(), somev![listv![nonev![]]]),
                        (
                            "C".to_string(),
                            somev![groupv![(
                                "d".to_string(),
                                somev![listv![
                                    somev![listv![
                                        somev![groupv![
                                            ("E".to_string(), nonev![]),
                                            ("F".to_string(), nonev![])
                                        ]],
                                        somev![groupv![
                                            ("E".to_string(), somev![Value::I32(10)]),
                                            (
                                                "F".to_string(),
                                                somev![Value::String("aaa".to_string())]
                                            )
                                        ]],
                                        somev![groupv![
                                            ("E".to_string(), nonev![]),
                                            ("F".to_string(), nonev![])
                                        ]],
                                        somev![groupv![
                                            ("E".to_string(), somev![Value::I32(-10)]),
                                            (
                                                "F".to_string(),
                                                somev![Value::String("bbb".to_string())]
                                            )
                                        ]],
                                        somev![groupv![
                                            ("E".to_string(), nonev![]),
                                            ("F".to_string(), nonev![])
                                        ]]
                                    ]],
                                    somev![listv![
                                        somev![groupv![
                                            ("E".to_string(), somev![Value::I32(11)]),
                                            (
                                                "F".to_string(),
                                                somev![Value::String("c".to_string())]
                                            )
                                        ]],
                                        nonev![]
                                    ]],
                                    somev![listv![]],
                                    nonev![]
                                ]]
                            )]]
                        ),
                        (
                            "g".to_string(),
                            somev![mapv![
                                (
                                    Value::String("g1".to_string()),
                                    somev![groupv![(
                                        "H".to_string(),
                                        somev![groupv![(
                                            "i".to_string(),
                                            somev![listv![somev![Value::F64(2.2)], nonev![]]]
                                        )]]
                                    )]]
                                ),
                                (
                                    Value::String("g2".to_string()),
                                    somev![groupv![(
                                        "H".to_string(),
                                        somev![groupv![("i".to_string(), somev![listv![]])]]
                                    )]]
                                ),
                                (Value::String("g3".to_string()), nonev![]),
                                (
                                    Value::String("g4".to_string()),
                                    somev![groupv![(
                                        "H".to_string(),
                                        somev![groupv![("i".to_string(), nonev![])]]
                                    )]]
                                ),
                                (
                                    Value::String("g5".to_string()),
                                    somev![groupv![("H".to_string(), nonev![])]]
                                )
                            ]]
                        )
                    ]]
                )
            ],
            row![
                ("id".to_string(), somev![Value::I64(3)]),
                ("int_array".to_string(), somev![listv![]]),
                ("int_array_Array".to_string(), somev![listv![nonev![]]]),
                ("int_map".to_string(), somev![mapv![]]),
                (
                    "int_Map_Array".to_string(),
                    somev![listv![nonev![], nonev![]]]
                ),
                (
                    "nested_struct".to_string(),
                    somev![groupv![
                        ("A".to_string(), nonev![]),
                        ("b".to_string(), nonev![]),
                        (
                            "C".to_string(),
                            somev![groupv![("d".to_string(), somev![listv![]])]]
                        ),
                        ("g".to_string(), somev![mapv![]])
                    ]]
                )
            ],
            row![
                ("id".to_string(), somev![Value::I64(4)]),
                ("int_array".to_string(), nonev![]),
                ("int_array_Array".to_string(), somev![listv![]]),
                ("int_map".to_string(), somev![mapv![]]),
                ("int_Map_Array".to_string(), somev![listv![]]),
                (
                    "nested_struct".to_string(),
                    somev![groupv![
                        ("A".to_string(), nonev![]),
                        ("b".to_string(), nonev![]),
                        (
                            "C".to_string(),
                            somev![groupv![("d".to_string(), nonev![])]]
                        ),
                        ("g".to_string(), nonev![])
                    ]]
                )
            ],
            row![
                ("id".to_string(), somev![Value::I64(5)]),
                ("int_array".to_string(), nonev![]),
                ("int_array_Array".to_string(), nonev![]),
                ("int_map".to_string(), somev![mapv![]]),
                ("int_Map_Array".to_string(), nonev![]),
                (
                    "nested_struct".to_string(),
                    somev![groupv![
                        ("A".to_string(), nonev![]),
                        ("b".to_string(), nonev![]),
                        ("C".to_string(), nonev![]),
                        (
                            "g".to_string(),
                            somev![mapv![(
                                Value::String("foo".to_string()),
                                somev![groupv![(
                                    "H".to_string(),
                                    somev![groupv![(
                                        "i".to_string(),
                                        somev![listv![
                                            somev![Value::F64(2.2)],
                                            somev![Value::F64(3.3)]
                                        ]]
                                    )]]
                                )]]
                            )]]
                        )
                    ]]
                )
            ],
            row![
                ("id".to_string(), somev![Value::I64(6)]),
                ("int_array".to_string(), nonev![]),
                ("int_array_Array".to_string(), nonev![]),
                ("int_map".to_string(), nonev![]),
                ("int_Map_Array".to_string(), nonev![]),
                ("nested_struct".to_string(), nonev![])
            ],
            row![
                ("id".to_string(), somev![Value::I64(7)]),
                ("int_array".to_string(), nonev![]),
                (
                    "int_array_Array".to_string(),
                    somev![listv![
                        nonev![],
                        somev![listv![somev![Value::I32(5)], somev![Value::I32(6)]]]
                    ]]
                ),
                (
                    "int_map".to_string(),
                    somev![mapv![
                        (Value::String("k1".to_string()), nonev![]),
                        (Value::String("k3".to_string()), nonev![])
                    ]]
                ),
                ("int_Map_Array".to_string(), nonev![]),
                (
                    "nested_struct".to_string(),
                    somev![groupv![
                        ("A".to_string(), somev![Value::I32(7)]),
                        (
                            "b".to_string(),
                            somev![listv![
                                somev![Value::I32(2)],
                                somev![Value::I32(3)],
                                nonev![]
                            ]]
                        ),
                        (
                            "C".to_string(),
                            somev![groupv![(
                                "d".to_string(),
                                somev![listv![
                                    somev![listv![]],
                                    somev![listv![nonev![]]],
                                    nonev![]
                                ]]
                            )]]
                        ),
                        ("g".to_string(), nonev![])
                    ]]
                )
            ],
        ];

        assert_eq!(rows, expected_rows);
    }

    #[test]
    fn test_file_reader_rows_nullable_typed() {
        type RowTyped = (
            Option<i64>,
            Option<List<Option<i32>>>,
            Option<List<Option<List<Option<i32>>>>>,
            Option<Map<String, Option<i32>>>,
            Option<List<Option<Map<String, Option<i32>>>>>,
            Option<(
                Option<i32>,
                Option<List<Option<i32>>>,
                Option<(Option<List<Option<List<Option<(Option<i32>, Option<String>)>>>>>,)>,
                Option<Map<String, Option<(Option<(Option<List<Option<f64>>>,)>,)>>>,
            )>,
        );

        let rows = test_file_reader_rows::<RowTyped>("nullable.impala.parquet", None).unwrap();

        let expected_rows: Vec<RowTyped> = vec![
            (
                Some(1),
                Some(list![Some(1), Some(2), Some(3)]),
                Some(list![
                    Some(list![Some(1), Some(2)]),
                    Some(list![Some(3), Some(4)])
                ]),
                Some(map![
                    ("k1".to_string(), Some(1)),
                    ("k2".to_string(), Some(100))
                ]),
                Some(list![Some(map![("k1".to_string(), Some(1))])]),
                Some((
                    Some(1),
                    Some(list![Some(1)]),
                    Some((Some(list![
                        Some(list![
                            Some((Some(10), Some("aaa".to_string()))),
                            Some((Some(-10), Some("bbb".to_string())))
                        ]),
                        Some(list![Some((Some(11), Some("c".to_string())))])
                    ]),)),
                    Some(map![(
                        "foo".to_string(),
                        Some((Some((Some(list![Some(1.1)]),)),))
                    )]),
                )),
            ),
            (
                Some(2),
                Some(list![None, Some(1), Some(2), None, Some(3), None]),
                Some(list![
                    Some(list![None, Some(1), Some(2), None]),
                    Some(list![Some(3), None, Some(4)]),
                    Some(list![]),
                    None
                ]),
                Some(map![("k1".to_string(), Some(2)), ("k2".to_string(), None)]),
                Some(list![
                    Some(map![("k3".to_string(), None), ("k1".to_string(), Some(1))]),
                    None,
                    Some(map![])
                ]),
                Some((
                    None,
                    Some(list![None]),
                    Some((Some(list![
                        Some(list![
                            Some((None, None)),
                            Some((Some(10), Some("aaa".to_string()))),
                            Some((None, None)),
                            Some((Some(-10), Some("bbb".to_string()))),
                            Some((None, None))
                        ]),
                        Some(list![Some((Some(11), Some("c".to_string()))), None]),
                        Some(list![]),
                        None
                    ]),)),
                    Some(map![
                        (
                            "g1".to_string(),
                            Some((Some((Some(list![Some(2.2), None]),)),))
                        ),
                        ("g2".to_string(), Some((Some((Some(list![]),)),))),
                        ("g3".to_string(), None),
                        ("g4".to_string(), Some((Some((None,)),))),
                        ("g5".to_string(), Some((None,)))
                    ]),
                )),
            ),
            (
                Some(3),
                Some(list![]),
                Some(list![None]),
                Some(map![]),
                Some(list![None, None]),
                Some((None, None, Some((Some(list![]),)), Some(map![]))),
            ),
            (
                Some(4),
                None,
                Some(list![]),
                Some(map![]),
                Some(list![]),
                Some((None, None, Some((None,)), None)),
            ),
            (
                Some(5),
                None,
                None,
                Some(map![]),
                None,
                Some((
                    None,
                    None,
                    None,
                    Some(map![(
                        "foo".to_string(),
                        Some((Some((Some(list![Some(2.2), Some(3.3)]),)),))
                    )]),
                )),
            ),
            (Some(6), None, None, None, None, None),
            (
                Some(7),
                None,
                Some(list![None, Some(list![Some(5), Some(6)])]),
                Some(map![("k1".to_string(), None), ("k3".to_string(), None)]),
                None,
                Some((
                    Some(7),
                    Some(list![Some(2), Some(3), None]),
                    Some((Some(list![Some(list![]), Some(list![None]), None]),)),
                    None,
                )),
            ),
        ];

        assert_eq!(rows, expected_rows);
    }

    // #[test]
    // fn test_file_reader_rows_projection() {
    //   let schema = "
    //     message spark_schema {
    //       REQUIRED DOUBLE c;
    //       REQUIRED INT32 b;
    //     }
    //   ";
    //   let schema = parse_message_type(&schema).unwrap();
    //   let rows = test_file_reader_rows::<Row>("nested_maps.snappy.parquet",
    // Some(schema)).unwrap();   let expected_rows = vec![
    //     row![
    //       ("c".to_string(), Value::F64(1.0)),
    //       ("b".to_string(), Value::I32(1))
    //     ],
    //     row![
    //       ("c".to_string(), Value::F64(1.0)),
    //       ("b".to_string(), Value::I32(1))
    //     ],
    //     row![
    //       ("c".to_string(), Value::F64(1.0)),
    //       ("b".to_string(), Value::I32(1))
    //     ],
    //     row![
    //       ("c".to_string(), Value::F64(1.0)),
    //       ("b".to_string(), Value::I32(1))
    //     ],
    //     row![
    //       ("c".to_string(), Value::F64(1.0)),
    //       ("b".to_string(), Value::I32(1))
    //     ],
    //     row![
    //       ("c".to_string(), Value::F64(1.0)),
    //       ("b".to_string(), Value::I32(1))
    //     ],
    //   ];
    //   assert_eq!(rows, expected_rows);
    // }

    // #[test]
    // fn test_file_reader_rows_projection_map() {
    //   let schema = "
    //     message spark_schema {
    //       OPTIONAL group a (MAP) {
    //         REPEATED group key_value {
    //           REQUIRED BYTE_ARRAY key (UTF8);
    //           OPTIONAL group value (MAP) {
    //             REPEATED group key_value {
    //               REQUIRED INT32 key;
    //               REQUIRED BOOLEAN value;
    //             }
    //           }
    //         }
    //       }
    //     }
    //   ";
    //   let schema = parse_message_type(&schema).unwrap();
    //   let rows = test_file_reader_rows::<Row>("nested_maps.snappy.parquet",
    // Some(schema)).unwrap();   let expected_rows = vec![
    //     row![(
    //       "a".to_string(),
    //       mapv![(
    //         Value::String("a".to_string()),
    //         mapv![
    //           (Value::I32(1), Value::Bool(true)),
    //           (Value::I32(2), Value::Bool(false))
    //         ]
    //       )]
    //     )],
    //     row![(
    //       "a".to_string(),
    //       mapv![(
    //         Value::String("b".to_string()),
    //         mapv![(Value::I32(1), Value::Bool(true))]
    //       )]
    //     )],
    //     row![(
    //       "a".to_string(),
    //       mapv![(Value::String("c".to_string()), nonev![])]
    //     )],
    //     row![("a".to_string(), mapv![(Value::String("d".to_string()), mapv![])])],
    //     row![(
    //       "a".to_string(),
    //       mapv![(
    //         Value::String("e".to_string()),
    //         mapv![(Value::I32(1), Value::Bool(true))]
    //       )]
    //     )],
    //     row![(
    //       "a".to_string(),
    //       mapv![(
    //         Value::String("f".to_string()),
    //         mapv![
    //           (Value::I32(3), Value::Bool(true)),
    //           (Value::I32(4), Value::Bool(false)),
    //           (Value::I32(5), Value::Bool(true))
    //         ]
    //       )]
    //     )],
    //   ];
    //   assert_eq!(rows, expected_rows);
    // }

    // #[test]
    // fn test_file_reader_rows_projection_list() {
    //   let schema = "
    //     message spark_schema {
    //       OPTIONAL group a (LIST) {
    //         REPEATED group list {
    //           OPTIONAL group element (LIST) {
    //             REPEATED group list {
    //               OPTIONAL group element (LIST) {
    //                 REPEATED group list {
    //                   OPTIONAL BYTE_ARRAY element (UTF8);
    //                 }
    //               }
    //             }
    //           }
    //         }
    //       }
    //     }
    //   ";
    //   let schema = parse_message_type(&schema).unwrap();
    //   let rows =
    //     test_file_reader_rows::<Row>("nested_lists.snappy.parquet",
    // Some(schema)).unwrap();   let expected_rows = vec![
    //     row![(
    //       "a".to_string(),
    //       listv![
    //         listv![
    //           listv![Value::String("a".to_string()), Value::String("b".to_string())],
    //           listv![Value::String("c".to_string())]
    //         ],
    //         listv![nonev![], listv![Value::String("d".to_string())]]
    //       ]
    //     )],
    //     row![(
    //       "a".to_string(),
    //       listv![
    //         listv![
    //           listv![Value::String("a".to_string()), Value::String("b".to_string())],
    //           listv![Value::String("c".to_string()), Value::String("d".to_string())]
    //         ],
    //         listv![nonev![], listv![Value::String("e".to_string())]]
    //       ]
    //     )],
    //     row![(
    //       "a".to_string(),
    //       listv![
    //         listv![
    //           listv![Value::String("a".to_string()), Value::String("b".to_string())],
    //           listv![Value::String("c".to_string()), Value::String("d".to_string())],
    //           listv![Value::String("e".to_string())]
    //         ],
    //         listv![nonev![], listv![Value::String("f".to_string())]]
    //       ]
    //     )],
    //   ];
    //   assert_eq!(rows, expected_rows);
    // }

    // #[test]
    // fn test_file_reader_rows_invalid_projection() {
    //   let schema = "
    //     message spark_schema {
    //       REQUIRED INT32 key;
    //       REQUIRED BOOLEAN value;
    //     }
    //   ";
    //   let schema = parse_message_type(&schema).unwrap();
    //   let res = test_file_reader_rows::<Row>("nested_maps.snappy.parquet", Some(schema));
    //   assert!(res.is_err());
    //   assert_eq!(
    //     res.unwrap_err(),
    //     general_err!("Root schema does not contain projection")
    //   );
    // }

    // #[test]
    // fn test_row_group_rows_invalid_projection() {
    //   let schema = "
    //     message spark_schema {
    //       REQUIRED INT32 key;
    //       REQUIRED BOOLEAN value;
    //     }
    //   ";
    //   let schema = parse_message_type(&schema).unwrap();
    //   let res = test_row_group_rows("nested_maps.snappy.parquet", Some(schema));
    //   assert!(res.is_err());
    //   assert_eq!(
    //     res.unwrap_err(),
    //     general_err!("Root schema does not contain projection")
    //   );
    // }

    // #[test]
    // #[should_panic(expected = "Invalid map type")]
    // fn test_file_reader_rows_invalid_map_type() {
    //   let schema = "
    //     message spark_schema {
    //       OPTIONAL group a (MAP) {
    //         REPEATED group key_value {
    //           REQUIRED BYTE_ARRAY key (UTF8);
    //           OPTIONAL group value (MAP) {
    //             REPEATED group key_value {
    //               REQUIRED INT32 key;
    //             }
    //           }
    //         }
    //       }
    //     }
    //   ";
    //   let schema = parse_message_type(&schema).unwrap();
    //   test_file_reader_rows::<Row>("nested_maps.snappy.parquet", Some(schema)).unwrap();
    // }

    #[test]
    fn test_tree_reader_handle_repeated_fields_with_no_annotation() {
        // Array field `phoneNumbers` does not contain LIST annotation.
        // We parse it as struct with `phone` repeated field as array.
        let rows = test_file_reader_rows::<Row>("repeated_no_annotation.parquet", None).unwrap();

        let expected_rows = vec![
            row![
                ("id".to_string(), Value::I32(1)),
                ("phoneNumbers".to_string(), nonev![])
            ],
            row![
                ("id".to_string(), Value::I32(2)),
                ("phoneNumbers".to_string(), nonev![])
            ],
            row![
                ("id".to_string(), Value::I32(3)),
                (
                    "phoneNumbers".to_string(),
                    somev![groupv![("phone".to_string(), listv![])]]
                )
            ],
            row![
                ("id".to_string(), Value::I32(4)),
                (
                    "phoneNumbers".to_string(),
                    somev![groupv![(
                        "phone".to_string(),
                        listv![groupv![
                            ("number".to_string(), Value::I64(5555555555)),
                            ("kind".to_string(), nonev![])
                        ]]
                    )]]
                )
            ],
            row![
                ("id".to_string(), Value::I32(5)),
                (
                    "phoneNumbers".to_string(),
                    somev![groupv![(
                        "phone".to_string(),
                        listv![groupv![
                            ("number".to_string(), Value::I64(1111111111)),
                            (
                                "kind".to_string(),
                                somev![Value::String("home".to_string())]
                            )
                        ]]
                    )]]
                )
            ],
            row![
                ("id".to_string(), Value::I32(6)),
                (
                    "phoneNumbers".to_string(),
                    somev![groupv![(
                        "phone".to_string(),
                        listv![
                            groupv![
                                ("number".to_string(), Value::I64(1111111111)),
                                (
                                    "kind".to_string(),
                                    somev![Value::String("home".to_string())]
                                )
                            ],
                            groupv![
                                ("number".to_string(), Value::I64(2222222222)),
                                ("kind".to_string(), nonev![])
                            ],
                            groupv![
                                ("number".to_string(), Value::I64(3333333333)),
                                (
                                    "kind".to_string(),
                                    somev![Value::String("mobile".to_string())]
                                )
                            ]
                        ]
                    )]]
                )
            ],
        ];

        assert_eq!(rows, expected_rows);
    }

    fn test_file_reader_rows<T>(file_name: &str, schema: Option<Type>) -> Result<Vec<T>>
    where
        Root<T>: Deserialize,
    {
        let file = get_test_file(file_name);
        let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
        let iter = file_reader.get_row_iter(schema)?;
        Ok(iter.collect())
    }

    fn test_row_group_rows(
        file_name: &str,
        schema: Option<Type>,
    ) -> Result<Vec<crate::record::types::Row>> {
        let file = get_test_file(file_name);
        let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
        // Check the first row group only, because files will contain only single row group
        let row_group_reader = file_reader.get_row_group(0).unwrap();
        // let iter = row_group_reader.get_row_iter(schema)?;
        let iter = RowIter::<SerializedFileReader<std::fs::File>, _>::from_row_group(
            schema,
            &*row_group_reader,
        )?;
        Ok(iter.collect())
    }
}

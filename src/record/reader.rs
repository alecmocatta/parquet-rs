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

use super::{
  types::{Group, List, Map, Root, Timestamp, Value},
  Deserialize,
};
use column::reader::ColumnReader;
use data_type::{
  BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type,
  Int64Type, Int96, Int96Type,
};
use errors::{ParquetError, Result};
use file::reader::{FileReader, RowGroupReader};
use record::triplet::TypedTripletIter;
use schema::types::{ColumnDescPtr, ColumnPath, SchemaDescPtr, SchemaDescriptor, Type};
use std::{
  collections::HashMap, convert::TryInto, error::Error, marker::PhantomData, rc::Rc,
};

// /// Tree builder for `Reader` enum.
// /// Serves as a container of options for building a reader tree and a builder, and
// /// accessing a records iterator [`RowIter`].
// pub struct TreeBuilder {
//   // Batch size (>= 1) for triplet iterators
//   batch_size: usize,
// }

// impl TreeBuilder {
//   /// Creates new tree builder with default parameters.
//   pub fn new() -> Self {
//     Self {
//       batch_size: DEFAULT_BATCH_SIZE,
//     }
//   }

//   /// Sets batch size for this tree builder.
//   pub fn with_batch_size(mut self, batch_size: usize) -> Self {
//     self.batch_size = batch_size;
//     self
//   }

//   /// Creates new root reader for provided schema and row group.
//   pub fn build(&self, descr: SchemaDescPtr, row_group_reader: &RowGroupReader) ->
// Reader {     // Prepare lookup table of column path -> original column index
//     // This allows to prune columns and map schema leaf nodes to the column readers
//     let mut paths: HashMap<ColumnPath, usize> = HashMap::new();
//     let row_group_metadata = row_group_reader.metadata();

//     for col_index in 0..row_group_reader.num_columns() {
//       let col_meta = row_group_metadata.column(col_index);
//       let col_path = col_meta.column_path().clone();
//       println!("path: {:?}", col_path);
//       paths.insert(col_path, col_index);
//     }

//     // Build child readers for the message type
//     let mut readers = Vec::new();
//     let mut path = Vec::new();

//     for field in descr.root_schema().get_fields() {
//       let reader =
//         self.reader_tree(field.clone(), &mut path, 0, 0, &paths, row_group_reader);
//       readers.push(reader);
//     }

//     // Return group reader for message type,
//     // it is always required with definition level 0
//     Reader::GroupReader(GroupReader{def_level: 0, readers})
//   }

//   /// Creates iterator of `Row`s directly from schema descriptor and row group.
//   pub fn as_iter(
//     &self,
//     descr: SchemaDescPtr,
//     row_group_reader: &RowGroupReader,
//   ) -> ReaderIter
//   {
//     let num_records = row_group_reader.metadata().num_rows() as usize;
//     ReaderIter::new(self.build(descr, row_group_reader), num_records)
//   }

//   /// Builds tree of readers for the current schema recursively.
//   fn reader_tree(
//     &self,
//     field: TypePtr,
//     mut path: &mut Vec<String>,
//     mut curr_def_level: i16,
//     mut curr_rep_level: i16,
//     paths: &HashMap<ColumnPath, usize>,
//     row_group_reader: &RowGroupReader,
//   ) -> Reader
//   {
//     assert!(field.get_basic_info().has_repetition());
//     // Update current definition and repetition levels for this type
//     let repetition = field.get_basic_info().repetition();
//     match repetition {
//       Repetition::OPTIONAL => {
//         curr_def_level += 1;
//       },
//       Repetition::REPEATED => {
//         curr_def_level += 1;
//         curr_rep_level += 1;
//       },
//       _ => {},
//     }

//     path.push(String::from(field.name()));
//     let reader = if field.is_primitive() {
//       let col_path = ColumnPath::new(path.to_vec());
//       let orig_index = *paths.get(&col_path).unwrap();
//       let col_descr = row_group_reader
//         .metadata()
//         .column(orig_index)
//         .column_descr_ptr();
//       let col_reader = row_group_reader.get_column_reader(orig_index).unwrap();
//       let (max_def_level, max_rep_level) = (col_descr.max_def_level(),
// col_descr.max_rep_level());       match col_descr.physical_type() {
//         PhysicalType::BOOLEAN => {
//           Reader::BoolReader(BoolReader{column:
// TypedTripletIter::<BoolType>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)})         },
//         PhysicalType::INT32 => {
//           Reader::I32Reader(I32Reader{column:
// TypedTripletIter::<Int32Type>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)})         },
//         PhysicalType::INT64 => {
//           Reader::I64Reader(I64Reader{column:
// TypedTripletIter::<Int64Type>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)})         },
//         PhysicalType::INT96 => {
//           Reader::I96Reader(I96Reader{column:
// TypedTripletIter::<Int96Type>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)})         },
//         PhysicalType::FLOAT => {
//           Reader::F32Reader(F32Reader{column:
// TypedTripletIter::<FloatType>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)})         },
//         PhysicalType::DOUBLE => {
//           Reader::F64Reader(F64Reader{column:
// TypedTripletIter::<DoubleType>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)})         },
//         PhysicalType::BYTE_ARRAY => Reader::ByteArrayReader(ByteArrayReader{column:
// TypedTripletIter::<ByteArrayType>::new(max_def_level, max_rep_level, self.batch_size,
// col_reader)}),         PhysicalType::FIXED_LEN_BYTE_ARRAY =>
// Reader::FixedLenByteArrayReader(FixedLenByteArrayReader{column:
// TypedTripletIter::<FixedLenByteArrayType>::new(max_def_level, max_rep_level,
// self.batch_size, col_reader)}),       }
//     } else {
//       match field.get_basic_info().logical_type() {
//         // List types
//         LogicalType::LIST => {
//           assert_eq!(field.get_fields().len(), 1, "Invalid list type {:?}", field);

//           let repeated_field = field.get_fields()[0].clone();
//           assert_eq!(
//             repeated_field.get_basic_info().repetition(),
//             Repetition::REPEATED,
//             "Invalid list type {:?}",
//             field
//           );

//           if Self::is_element_type(&repeated_field) {
//             // Support for backward compatible lists
//             let reader = self.reader_tree(
//               repeated_field.clone(),
//               &mut path,
//               curr_def_level,
//               curr_rep_level,
//               paths,
//               row_group_reader,
//             );

//             Reader::RepeatedReader(
//               RepeatedReader{
//                 def_level: curr_def_level,
//                 rep_level: curr_rep_level,
//                 reader: Box::new(reader),
//               }
//             )
//           } else {
//             let child_field = repeated_field.get_fields()[0].clone();

//             path.push(String::from(repeated_field.name()));

//             let reader = self.reader_tree(
//               child_field,
//               &mut path,
//               curr_def_level + 1,
//               curr_rep_level + 1,
//               paths,
//               row_group_reader,
//             );

//             path.pop();

//             Reader::RepeatedReader(
//               RepeatedReader{
//                 def_level: curr_def_level,
//                 rep_level: curr_rep_level,
//                 reader: Box::new(reader),
//               }
//             )
//           }
//         },
//         // Map types (key-value pairs)
//         LogicalType::MAP | LogicalType::MAP_KEY_VALUE => {
//           assert_eq!(field.get_fields().len(), 1, "Invalid map type: {:?}", field);
//           assert!(
//             !field.get_fields()[0].is_primitive(),
//             "Invalid map type: {:?}",
//             field
//           );

//           let key_value_type = field.get_fields()[0].clone();
//           assert_eq!(
//             key_value_type.get_basic_info().repetition(),
//             Repetition::REPEATED,
//             "Invalid map type: {:?}",
//             field
//           );
//           assert_eq!(
//             key_value_type.get_fields().len(),
//             2,
//             "Invalid map type: {:?}",
//             field
//           );

//           path.push(String::from(key_value_type.name()));

//           let key_type = &key_value_type.get_fields()[0];
//           assert!(
//             key_type.is_primitive(),
//             "Map key type is expected to be a primitive type, but found {:?}",
//             key_type
//           );
//           let key_reader = self.reader_tree(
//             key_type.clone(),
//             &mut path,
//             curr_def_level + 1,
//             curr_rep_level + 1,
//             paths,
//             row_group_reader,
//           );

//           let value_type = &key_value_type.get_fields()[1];
//           let value_reader = self.reader_tree(
//             value_type.clone(),
//             &mut path,
//             curr_def_level + 1,
//             curr_rep_level + 1,
//             paths,
//             row_group_reader,
//           );

//           path.pop();

//           Reader::KeyValueReader(
//             KeyValueReader{
//               def_level: curr_def_level,
//               rep_level: curr_rep_level,
//               keys_reader: Box::new(key_reader),
//               values_reader: Box::new(value_reader),
//             }
//           )
//         },
//         // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated
//         // group nor annotated by `LIST` or `MAP` should be interpreted as a required
//         // list of required elements where the element type is the type of the field.
//         _ if repetition == Repetition::REPEATED => {
//           let required_field = Type::group_type_builder(field.name())
//             .with_repetition(Repetition::REQUIRED)
//             .with_logical_type(field.get_basic_info().logical_type())
//             .with_fields(&mut Vec::from(field.get_fields()))
//             .build()
//             .unwrap();

//           path.pop();

//           let reader = self.reader_tree(
//             Rc::new(required_field),
//             &mut path,
//             curr_def_level,
//             curr_rep_level,
//             paths,
//             row_group_reader,
//           );

//           Reader::RepeatedReader(
//             RepeatedReader{
//               def_level: curr_def_level - 1,
//               rep_level: curr_rep_level - 1,
//               reader: Box::new(reader),
//             }
//           )
//         },
//         // Group types (structs)
//         _ => {
//           let mut readers = Vec::new();
//           for child in field.get_fields() {
//             let reader = self.reader_tree(
//               child.clone(),
//               &mut path,
//               curr_def_level,
//               curr_rep_level,
//               paths,
//               row_group_reader,
//             );
//             readers.push(reader);
//           }
//           Reader::GroupReader(GroupReader{def_level: curr_def_level, readers})
//         },
//       }
//     };
//     path.pop();

//     if repetition == Repetition::OPTIONAL {
//       Reader::OptionReader(OptionReader{def_level: curr_def_level - 1, reader:
// Box::new(reader)})     } else {
//       reader
//     }
//   }

//   /// Returns true if repeated type is an element type for the list.
//   /// Used to determine legacy list types.
//   /// This method is copied from Spark Parquet reader and is based on the reference:
//   /// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
//   ///   #backward-compatibility-rules
//   fn is_element_type(repeated_type: &Type) -> bool {
//     // For legacy 2-level list types with primitive element type, e.g.:
//     //
//     //    // ARRAY<INT> (nullable list, non-null elements)
//     //    optional group my_list (LIST) {
//     //      repeated int32 element;
//     //    }
//     //
//     repeated_type.is_primitive() ||
//     // For legacy 2-level list types whose element type is a group type with 2 or more
//     // fields, e.g.:
//     //
//     //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
//     //    optional group my_list (LIST) {
//     //      repeated group element {
//     //        required binary str (UTF8);
//     //        required int32 num;
//     //      };
//     //    }
//     //
//     repeated_type.is_group() && repeated_type.get_fields().len() > 1 ||
//     // For legacy 2-level list types generated by parquet-avro (Parquet version <
// 1.6.0),     // e.g.:
//     //
//     //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
//     //    optional group my_list (LIST) {
//     //      repeated group array {
//     //        required binary str (UTF8);
//     //      };
//     //    }
//     //
//     repeated_type.name() == "array" ||
//     // For Parquet data generated by parquet-thrift, e.g.:
//     //
//     //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
//     //    optional group my_list (LIST) {
//     //      repeated group my_list_tuple {
//     //        required binary str (UTF8);
//     //      };
//     //    }
//     //
//     repeated_type.name().ends_with("_tuple")
//   }
// }

impl<A, B, C> Reader for sum::Sum3<A, B, C>
where
  A: Reader,
  B: Reader<Item = A::Item>,
  C: Reader<Item = A::Item>,
{
  type Item = A::Item;

  fn read_field(&mut self) -> Result<Self::Item> {
    match self {
      sum::Sum3::A(ref mut reader) => reader.read_field(),
      sum::Sum3::B(ref mut reader) => reader.read_field(),
      sum::Sum3::C(ref mut reader) => reader.read_field(),
    }
  }

  fn advance_columns(&mut self) {
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
  pub column: TypedTripletIter<BoolType>,
}
pub struct I32Reader {
  pub column: TypedTripletIter<Int32Type>,
}
pub struct I64Reader {
  pub column: TypedTripletIter<Int64Type>,
}
pub struct I96Reader {
  pub column: TypedTripletIter<Int96Type>,
}
pub struct F32Reader {
  pub column: TypedTripletIter<FloatType>,
}
pub struct F64Reader {
  pub column: TypedTripletIter<DoubleType>,
}
pub struct ByteArrayReader {
  pub column: TypedTripletIter<ByteArrayType>,
}
pub struct FixedLenByteArrayReader {
  pub column: TypedTripletIter<FixedLenByteArrayType>,
}
pub struct OptionReader<R> {
  pub def_level: i16,
  pub reader: R,
}
pub struct RepeatedReader<R> {
  pub def_level: i16,
  pub rep_level: i16,
  pub reader: R,
}
pub struct KeyValueReader<K, V> {
  pub def_level: i16,
  pub rep_level: i16,
  pub keys_reader: K,
  pub values_reader: V,
}

pub trait Reader {
  type Item;
  fn read_field(&mut self) -> Result<Self::Item>;
  fn advance_columns(&mut self);
  fn has_next(&self) -> bool;
  fn current_def_level(&self) -> i16;
  fn current_rep_level(&self) -> i16;
}

impl Reader for BoolReader {
  type Item = bool;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = *self.column.current_value();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for I32Reader {
  type Item = i32;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = *self.column.current_value();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for I64Reader {
  type Item = i64;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = *self.column.current_value();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for I96Reader {
  type Item = Int96;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = self.column.current_value().clone();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for F32Reader {
  type Item = f32;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = *self.column.current_value();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for F64Reader {
  type Item = f64;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = *self.column.current_value();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for ByteArrayReader {
  type Item = Vec<u8>;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = self.column.current_value().data().to_owned();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl Reader for FixedLenByteArrayReader {
  type Item = Vec<u8>;

  fn read_field(&mut self) -> Result<Self::Item> {
    let value = self.column.current_value().data().to_owned();
    self.column.read_next().unwrap();
    Ok(value)
  }

  fn advance_columns(&mut self) { self.column.read_next().unwrap(); }

  fn has_next(&self) -> bool { self.column.has_next() }

  fn current_def_level(&self) -> i16 { self.column.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.column.current_rep_level() }
}
impl<R: Reader> Reader for OptionReader<R> {
  type Item = Option<R::Item>;

  fn read_field(&mut self) -> Result<Self::Item> {
    if self.reader.current_def_level() > self.def_level {
      self.reader.read_field().map(Some)
    } else {
      self.reader.advance_columns();
      Ok(None)
    }
  }

  fn advance_columns(&mut self) { self.reader.advance_columns(); }

  fn has_next(&self) -> bool { self.reader.has_next() }

  fn current_def_level(&self) -> i16 { self.reader.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.reader.current_rep_level() }
}

impl<R: Reader> Reader for RepeatedReader<R> {
  type Item = Vec<R::Item>;

  fn read_field(&mut self) -> Result<Self::Item> {
    let mut elements = Vec::new();
    loop {
      if self.reader.current_def_level() > self.def_level {
        elements.push(self.reader.read_field()?);
      } else {
        self.reader.advance_columns();
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

  fn advance_columns(&mut self) { self.reader.advance_columns(); }

  fn has_next(&self) -> bool { self.reader.has_next() }

  fn current_def_level(&self) -> i16 { self.reader.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.reader.current_rep_level() }
}

impl<K: Reader, V: Reader> Reader for KeyValueReader<K, V> {
  type Item = Vec<(K::Item, V::Item)>;

  fn read_field(&mut self) -> Result<Self::Item> {
    let mut pairs = Vec::new();
    loop {
      if self.keys_reader.current_def_level() > self.def_level {
        pairs.push((
          self.keys_reader.read_field()?,
          self.values_reader.read_field()?,
        ));
      } else {
        self.keys_reader.advance_columns();
        self.values_reader.advance_columns();
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

  fn advance_columns(&mut self) {
    self.keys_reader.advance_columns();
    self.values_reader.advance_columns();
  }

  fn has_next(&self) -> bool { self.keys_reader.has_next() }

  fn current_def_level(&self) -> i16 { self.keys_reader.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.keys_reader.current_rep_level() }
}

pub struct GroupReader {
  pub(super) def_level: i16,
  pub(super) readers: Vec<ValueReader>,
  pub(super) fields: Rc<HashMap<String, usize>>,
}
impl Reader for GroupReader {
  type Item = Group;

  fn read_field(&mut self) -> Result<Self::Item> {
    let mut fields = Vec::new();
    for reader in self.readers.iter_mut() {
      fields.push(reader.read_field()?);
    }
    Ok(Group(fields, self.fields.clone()))
  }

  fn advance_columns(&mut self) {
    for reader in self.readers.iter_mut() {
      reader.advance_columns();
    }
  }

  fn has_next(&self) -> bool { self.readers.first().unwrap().has_next() }

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

  fn read_field(&mut self) -> Result<Self::Item> {
    match self {
      ValueReader::Bool(ref mut reader) => reader.read_field().map(Value::Bool),
      ValueReader::U8(ref mut reader) => reader.read_field().map(Value::U8),
      ValueReader::I8(ref mut reader) => reader.read_field().map(Value::I8),
      ValueReader::U16(ref mut reader) => reader.read_field().map(Value::U16),
      ValueReader::I16(ref mut reader) => reader.read_field().map(Value::I16),
      ValueReader::U32(ref mut reader) => reader.read_field().map(Value::U32),
      ValueReader::I32(ref mut reader) => reader.read_field().map(Value::I32),
      ValueReader::U64(ref mut reader) => reader.read_field().map(Value::U64),
      ValueReader::I64(ref mut reader) => reader.read_field().map(Value::I64),
      ValueReader::F32(ref mut reader) => reader.read_field().map(Value::F32),
      ValueReader::F64(ref mut reader) => reader.read_field().map(Value::F64),
      ValueReader::Timestamp(ref mut reader) => reader.read_field().map(Value::Timestamp),
      ValueReader::Array(ref mut reader) => reader.read_field().map(Value::Array),
      ValueReader::String(ref mut reader) => reader.read_field().map(Value::String),
      ValueReader::List(ref mut reader) => reader.read_field().map(Value::List),
      ValueReader::Map(ref mut reader) => reader.read_field().map(Value::Map),
      ValueReader::Group(ref mut reader) => reader.read_field().map(Value::Group),
      ValueReader::Option(ref mut reader) => {
        reader.read_field().map(|x| Value::Option(Box::new(x)))
      },
    }
  }

  fn advance_columns(&mut self) {
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

pub struct TupleReader<T>(pub(super) T);

pub struct TryIntoReader<R: Reader, T>(pub(super) R, pub(super) PhantomData<fn(T)>);
impl<R: Reader, T> Reader for TryIntoReader<R, T>
where
  R::Item: TryInto<T>,
  <R::Item as TryInto<T>>::Error: Error,
{
  type Item = T;

  fn read_field(&mut self) -> Result<Self::Item> {
    self.0.read_field().and_then(|x| {
      x.try_into()
        .map_err(|err| ParquetError::General(err.description().to_owned()))
    })
  }

  fn advance_columns(&mut self) { self.0.advance_columns() }

  fn has_next(&self) -> bool { self.0.has_next() }

  fn current_def_level(&self) -> i16 { self.0.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.0.current_rep_level() }
}

pub struct MapReader<R: Reader, F>(pub(super) R, pub(super) F);
impl<R: Reader, F, T> Reader for MapReader<R, F>
where F: FnMut(R::Item) -> Result<T>
{
  type Item = T;

  fn read_field(&mut self) -> Result<Self::Item> {
    self.0.read_field().and_then(&mut self.1)
  }

  fn advance_columns(&mut self) { self.0.advance_columns() }

  fn has_next(&self) -> bool { self.0.has_next() }

  fn current_def_level(&self) -> i16 { self.0.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.0.current_rep_level() }
}

pub struct RootReader<R>(pub R);
impl<R> Reader for RootReader<R>
where R: Reader
{
  type Item = Root<R::Item>;

  fn read_field(&mut self) -> Result<Self::Item> { self.0.read_field().map(Root) }

  fn advance_columns(&mut self) { self.0.advance_columns(); }

  fn has_next(&self) -> bool { self.0.has_next() }

  fn current_def_level(&self) -> i16 { self.0.current_def_level() }

  fn current_rep_level(&self) -> i16 { self.0.current_rep_level() }
}

use super::DebugType;
use std::fmt::{self, Debug};

struct DebugDebugType<T>(PhantomData<fn(T)>)
where T: DebugType;
impl<T> DebugDebugType<T>
where T: DebugType
{
  fn new() -> Self { Self(PhantomData) }
}
impl<T> Debug for DebugDebugType<T>
where T: DebugType
{
  fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
    T::fmt(f)
  }
}

// ----------------------------------------------------------------------
// Row iterators

/// Iterator of [`Row`](`::record::api::Row`)s.
/// It is used either for a single row group to iterate over data in that row group, or
/// an entire file with auto buffering of all row groups.
pub struct RowIter<'a, R, T>
where
  R: FileReader,
  Root<T>: Deserialize, {
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
  /// Creates iterator of [`Row`](`::record::api::Row`)s for all row groups in a file.
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
          "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{:#?}\nError: \
           {}",
          String::from_utf8(b).unwrap(),
          // String::from_utf8(a).unwrap(),
          DebugDebugType::<<Root<T> as Deserialize>::Schema>::new(),
          err
        ))
      })
      .unwrap()
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

  /// Creates iterator of [`Row`](`::record::api::Row`)s for a specific row group.
  pub fn from_row_group(
    proj: Option<Type>,
    row_group_reader: &'a RowGroupReader,
  ) -> Result<Self>
  {
    let descr =
      Self::get_proj_descr(proj, row_group_reader.metadata().schema_descr_ptr())?;

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
          "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{:#?}\nError: \
           {}",
          String::from_utf8(b).unwrap(),
          // String::from_utf8(a).unwrap(),
          DebugDebugType::<<Root<T> as Deserialize>::Schema>::new(),
          err
        ))
      })
      .unwrap()
      .1;

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
    let reader = <Root<T>>::reader(&schema, &mut path, 0, 0, &mut paths);
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
  fn get_proj_descr(
    proj: Option<Type>,
    root_descr: SchemaDescPtr,
  ) -> Result<SchemaDescPtr>
  {
    match proj {
      Some(projection) => {
        // check if projection is part of file schema
        let root_schema = root_descr.root_schema();
        if !root_schema.check_contains(&projection) {
          return Err(general_err!("Root schema does not contain projection"));
        }
        Ok(Rc::new(SchemaDescriptor::new(Rc::new(projection))))
      },
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
      let reader = <Root<T>>::reader(&self.schema, &mut path, 0, 0, &mut paths);
      let mut row_iter =
        ReaderIter::new(reader, row_group_reader.metadata().num_rows() as u64);

      row = row_iter.next();
      self.row_iter = Some(row_iter);
    }

    row
  }
}

/// Internal iterator of [`Row`](`::record::api::Row`)s for a reader.
struct ReaderIter<T>
where Root<T>: Deserialize {
  root_reader: <Root<T> as Deserialize>::Reader,
  records_left: u64,
  marker: PhantomData<fn() -> T>,
}

impl<T> ReaderIter<T>
where Root<T>: Deserialize
{
  fn new(mut root_reader: <Root<T> as Deserialize>::Reader, num_records: u64) -> Self {
    // Prepare root reader by advancing all column vectors
    root_reader.advance_columns();
    Self {
      root_reader,
      records_left: num_records,
      marker: PhantomData,
    }
  }
}

impl<T> Iterator for ReaderIter<T>
where Root<T>: Deserialize
{
  type Item = T;

  fn next(&mut self) -> Option<T> {
    if self.records_left > 0 {
      self.records_left -= 1;
      Some(self.root_reader.read_field().unwrap().0)
    } else {
      None
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use errors::{ParquetError, Result};
  use file::reader::{FileReader, SerializedFileReader};
  use record::types::Value;
  use schema::parser::parse_message_type;
  use util::test_common::get_test_file;

  // Convenient macros to assemble row, list, map, and group.

  macro_rules! row {
    ( $( ($name:expr, $e:expr) ), * ) => {
      {
        group!($( ($name, $e) ), *)
      }
    }
  }

  macro_rules! list {
    ( $( $e:expr ), * ) => {
      {
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

  macro_rules! group {
    ( $( ($name:expr, $e:expr) ), * ) => {
      {
        let mut result = Vec::new();
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
      Value::Group(group!($(($name,$e)),*))
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
    let rows = test_file_reader_rows("nulls.snappy.parquet", None).unwrap();
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
  fn test_file_reader_rows_nonnullable() {
    let rows = test_file_reader_rows("nonnullable.impala.parquet", None).unwrap();
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
  fn test_file_reader_rows_nullable() {
    let rows = test_file_reader_rows("nullable.impala.parquet", None).unwrap();
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
                      ("F".to_string(), somev![Value::String("aaa".to_string())])
                    ]],
                    somev![groupv![
                      ("E".to_string(), somev![Value::I32(-10)]),
                      ("F".to_string(), somev![Value::String("bbb".to_string())])
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
                      ("F".to_string(), somev![Value::String("aaa".to_string())])
                    ]],
                    somev![groupv![
                      ("E".to_string(), nonev![]),
                      ("F".to_string(), nonev![])
                    ]],
                    somev![groupv![
                      ("E".to_string(), somev![Value::I32(-10)]),
                      ("F".to_string(), somev![Value::String("bbb".to_string())])
                    ]],
                    somev![groupv![
                      ("E".to_string(), nonev![]),
                      ("F".to_string(), nonev![])
                    ]]
                  ]],
                  somev![listv![
                    somev![groupv![
                      ("E".to_string(), somev![Value::I32(11)]),
                      ("F".to_string(), somev![Value::String("c".to_string())])
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
                    somev![listv![somev![Value::F64(2.2)], somev![Value::F64(3.3)]]]
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
                somev![listv![somev![listv![]], somev![listv![nonev![]]], nonev![]]]
              )]]
            ),
            ("g".to_string(), nonev![])
          ]]
        )
      ],
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
  //   let rows = test_file_reader_rows("nested_maps.snappy.parquet",
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
  //   let rows = test_file_reader_rows("nested_maps.snappy.parquet",
  // Some(schema)).unwrap();   let expected_rows = vec![
  //     row![(
  //       "a".to_string(),
  //       mapv![(
  //         Value::String("a".to_string()),
  //         map![
  //           (Value::I32(1), Value::Bool(true)),
  //           (Value::I32(2), Value::Bool(false))
  //         ]
  //       )]
  //     )],
  //     row![(
  //       "a".to_string(),
  //       map![(
  //         Value::String("b".to_string()),
  //         map![(Value::I32(1), Value::Bool(true))]
  //       )]
  //     )],
  //     row![(
  //       "a".to_string(),
  //       map![(Value::String("c".to_string()), nonev![])]
  //     )],
  //     row![("a".to_string(), map![(Value::String("d".to_string()), map![])])],
  //     row![(
  //       "a".to_string(),
  //       map![(
  //         Value::String("e".to_string()),
  //         map![(Value::I32(1), Value::Bool(true))]
  //       )]
  //     )],
  //     row![(
  //       "a".to_string(),
  //       map![(
  //         Value::String("f".to_string()),
  //         map![
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
  //     test_file_reader_rows("nested_lists.snappy.parquet", Some(schema)).unwrap();
  //   let expected_rows = vec![
  //     row![(
  //       "a".to_string(),
  //       list![
  //         list![
  //           list![Value::String("a".to_string()), Value::String("b".to_string())],
  //           list![Value::String("c".to_string())]
  //         ],
  //         list![nonev![], list![Value::String("d".to_string())]]
  //       ]
  //     )],
  //     row![(
  //       "a".to_string(),
  //       list![
  //         list![
  //           list![Value::String("a".to_string()), Value::String("b".to_string())],
  //           list![Value::String("c".to_string()), Value::String("d".to_string())]
  //         ],
  //         list![nonev![], list![Value::String("e".to_string())]]
  //       ]
  //     )],
  //     row![(
  //       "a".to_string(),
  //       list![
  //         list![
  //           list![Value::String("a".to_string()), Value::String("b".to_string())],
  //           list![Value::String("c".to_string()), Value::String("d".to_string())],
  //           list![Value::String("e".to_string())]
  //         ],
  //         list![nonev![], list![Value::String("f".to_string())]]
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
  //   let res = test_file_reader_rows("nested_maps.snappy.parquet", Some(schema));
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
  //   test_file_reader_rows("nested_maps.snappy.parquet", Some(schema)).unwrap();
  // }

  #[test]
  fn test_tree_reader_handle_repeated_fields_with_no_annotation() {
    // Array field `phoneNumbers` does not contain LIST annotation.
    // We parse it as struct with `phone` repeated field as array.
    let rows = test_file_reader_rows("repeated_no_annotation.parquet", None).unwrap();
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

  fn test_file_reader_rows(
    file_name: &str,
    schema: Option<Type>,
  ) -> Result<Vec<crate::record::types::Row>>
  {
    let file = get_test_file(file_name);
    let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
    let iter = file_reader.get_row_iter(schema)?;
    Ok(iter.collect())
  }

  fn test_row_group_rows(
    file_name: &str,
    schema: Option<Type>,
  ) -> Result<Vec<crate::record::types::Row>>
  {
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

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

use column::reader::{get_typed_column_reader, ColumnReader, ColumnReaderImpl};
use data_type::*;
use errors::{ParquetError, Result};
use record::{
  reader::{Reader, ValueReader},
  types::Value,
  Deserialize,
};
use schema::types::{ColumnDescPtr, ColumnPath};
use std::mem;

// /// Macro to generate simple functions that cover all types of triplet iterator.
// /// $func is a function of a typed triplet iterator and $token is a either {`ref`} or
// /// {`ref`, `mut`}
// macro_rules! triplet_enum_func {
//   ($self:ident, $func:ident, $( $token:tt ),*) => ({
//     match *$self {
//       TripletIter::BoolTripletIter($($token)* typed) => typed.$func(),
//       TripletIter::Int32TripletIter($($token)* typed) => typed.$func(),
//       TripletIter::Int64TripletIter($($token)* typed) => typed.$func(),
//       TripletIter::Int96TripletIter($($token)* typed) => typed.$func(),
//       TripletIter::FloatTripletIter($($token)* typed) => typed.$func(),
//       TripletIter::DoubleTripletIter($($token)* typed) => typed.$func(),
//       TripletIter::ByteArrayTripletIter($($token)* typed) => typed.$func(),
//       TripletIter::FixedLenByteArrayTripletIter($($token)* typed) => typed.$func()
//     }
//   });
// }

/// High level API wrapper on column reader.
/// Provides per-element access for each primitive column.
pub struct TripletIter(ValueReader);

impl TripletIter {
  /// Creates new triplet for column reader
  pub fn new(descr: ColumnDescPtr, reader: ColumnReader, batch_size: usize) -> Self {
    let schema = Value::parse(descr.self_type()).unwrap().1;
    let mut def = descr.max_def_level();
    let mut rep = descr.max_rep_level();
    match descr.self_type().get_basic_info().repetition() {
      crate::basic::Repetition::REQUIRED => (),
      crate::basic::Repetition::OPTIONAL => def -= 1,
      crate::basic::Repetition::REPEATED => {
        def -= 1;
        rep -= 1
      },
    }
    let reader = Value::reader(
      &schema,
      &mut vec![],
      def,
      rep,
      &mut vec![(ColumnPath::new(vec![]), (descr.clone(), reader))]
        .into_iter()
        .collect(),
    );
    TripletIter(reader)
  }

  /// Invokes underlying typed triplet iterator to buffer current value.
  /// Should be called once - either before `is_null` or `current_value`.
  #[inline]
  pub fn advance_columns(&mut self) -> Result<bool> {
    self.0.advance_columns();
    Ok(self.0.has_next())
  }

  /// Provides check on values/levels left without invoking the underlying typed triplet
  /// iterator.
  /// Returns true if more values/levels exist, false otherwise.
  /// It is always in sync with `advance_columns` method.
  #[inline]
  pub fn has_next(&self) -> bool { self.0.has_next() }

  /// Returns current definition level for a leaf triplet iterator
  #[inline]
  pub fn current_def_level(&self) -> i16 { self.0.current_def_level() }

  // /// Returns max definition level for a leaf triplet iterator
  // #[inline]
  // pub fn max_def_level(&self) -> i16 { triplet_enum_func!(self, max_def_level, ref) }

  /// Returns current repetition level for a leaf triplet iterator
  #[inline]
  pub fn current_rep_level(&self) -> i16 { self.0.current_rep_level() }

  // /// Returns max repetition level for a leaf triplet iterator
  // #[inline]
  // pub fn max_rep_level(&self) -> i16 { triplet_enum_func!(self, max_rep_level, ref) }

  // /// Returns true, if current value is null.
  // /// Based on the fact that for non-null value current definition level
  // /// equals to max definition level.
  // #[inline]
  // pub fn is_null(&self) -> bool { self.current_def_level() < self.max_def_level() }

  /// Updates non-null value for current row.
  pub fn read(&mut self) -> Result<Value> {
    // assert!(!self.is_null(), "Value is null");
    self.0.read()
  }
}

/// Internal typed triplet iterator as a wrapper for column reader
/// (primitive leaf column), provides per-element access.
pub struct TypedTripletIter<T: DataType> {
  reader: ColumnReaderImpl<T>,
  batch_size: usize,
  // type properties
  max_def_level: i16,
  max_rep_level: i16,
  // values and levels
  values: Vec<T::T>,
  def_levels: Option<Vec<i16>>,
  rep_levels: Option<Vec<i16>>,
  // current index for the triplet (value, def, rep)
  curr_triplet_index: usize,
  // how many triplets are left before we need to buffer
  triplets_left: usize,
  // helper flag to quickly check if we have more values/levels to read
  has_next: bool,
}

impl<T: DataType> TypedTripletIter<T> {
  /// Creates new typed triplet iterator based on provided column reader.
  /// Use batch size to specify the amount of values to buffer from column reader.
  pub fn new(
    max_def_level: i16,
    max_rep_level: i16,
    batch_size: usize,
    column_reader: ColumnReader,
  ) -> Self
  {
    assert_ne!(batch_size, 0, "Expected positive batch size");

    let def_levels = if max_def_level == 0 {
      None
    } else {
      Some(vec![0; batch_size])
    };
    let rep_levels = if max_rep_level == 0 {
      None
    } else {
      Some(vec![0; batch_size])
    };

    Self {
      reader: get_typed_column_reader(column_reader),
      batch_size,
      max_def_level,
      max_rep_level,
      values: vec![T::T::default(); batch_size],
      def_levels,
      rep_levels,
      curr_triplet_index: 0,
      triplets_left: 0,
      has_next: false,
    }
  }

  /// Returns maximum definition level for the triplet iterator (leaf column).
  #[inline]
  pub fn max_def_level(&self) -> i16 { self.max_def_level }

  /// Returns maximum repetition level for the triplet iterator (leaf column).
  #[inline]
  pub fn max_rep_level(&self) -> i16 { self.max_rep_level }

  /// Returns current value, advancing the iterator.
  #[inline]
  pub fn read(&mut self) -> Result<T::T> {
    assert_eq!(
      self.current_def_level(),
      self.max_def_level,
      "Cannot extract value, max definition level: {}, current level: {}",
      self.max_def_level,
      self.current_def_level()
    );
    let ret = mem::replace(&mut self.values[self.curr_triplet_index], T::T::default());
    self.advance_columns().unwrap();
    Ok(ret)
  }

  /// Returns current definition level.
  /// If field is required, then maximum definition level is returned.
  #[inline]
  pub fn current_def_level(&self) -> i16 {
    match self.def_levels {
      Some(ref vec) => vec[self.curr_triplet_index],
      None => self.max_def_level,
    }
  }

  /// Returns current repetition level.
  /// If field is required, then maximum repetition level is returned.
  #[inline]
  pub fn current_rep_level(&self) -> i16 {
    match self.rep_levels {
      Some(ref vec) => vec[self.curr_triplet_index],
      None => self.max_rep_level,
    }
  }

  /// Quick check if iterator has more values/levels to read.
  /// It is updated as a result of `advance_columns` method, so they are synchronized.
  #[inline]
  pub fn has_next(&self) -> bool { self.has_next }

  /// Advances to the next triplet.
  /// Returns true, if there are more records to read, false there are no records left.
  pub fn advance_columns(&mut self) -> Result<bool> {
    self.curr_triplet_index += 1;

    if self.curr_triplet_index >= self.triplets_left {
      let (values_read, levels_read) = {
        // Get slice of definition levels, if available
        let def_levels = match self.def_levels {
          Some(ref mut vec) => Some(&mut vec[..]),
          None => None,
        };

        // Get slice of repetition levels, if available
        let rep_levels = match self.rep_levels {
          Some(ref mut vec) => Some(&mut vec[..]),
          None => None,
        };

        // Buffer triplets
        self.reader.read_batch(
          self.batch_size,
          def_levels,
          rep_levels,
          &mut self.values,
        )?
      };

      // No more values or levels to read
      if values_read == 0 && levels_read == 0 {
        self.has_next = false;
        return Ok(false);
      }

      // We never read values more than levels
      if levels_read == 0 || values_read == levels_read {
        // There are no definition levels to read, column is required
        // or definition levels match values, so it does not require spacing
        self.curr_triplet_index = 0;
        self.triplets_left = values_read;
      } else if values_read < levels_read {
        // Add spacing for triplets.
        // The idea is setting values for positions in def_levels when current definition
        // level equals to maximum definition level. Values and levels are guaranteed to
        // line up, because of the column reader method.

        // Note: if values_read == 0, then spacing will not be triggered
        let mut idx = values_read;
        let def_levels = self.def_levels.as_ref().unwrap();
        for i in 0..levels_read {
          if def_levels[levels_read - i - 1] == self.max_def_level {
            idx -= 1; // This is done to avoid usize becoming a negative value
            self.values.swap(levels_read - i - 1, idx);
          }
        }
        self.curr_triplet_index = 0;
        self.triplets_left = levels_read;
      } else {
        return Err(general_err!(
          "Spacing of values/levels is wrong, values_read: {}, levels_read: {}",
          values_read,
          levels_read
        ));
      }
    }

    self.has_next = true;
    Ok(true)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use file::reader::{FileReader, SerializedFileReader};
  use schema::types::ColumnPath;
  use util::test_common::get_test_file;

  // #[test]
  // #[should_panic(expected = "Expected positive batch size")]
  // fn test_triplet_zero_batch_size() {
  //   let column_path =
  //     ColumnPath::from(vec!["b_struct".to_string(), "b_c_int".to_string()]);
  //   test_column_in_file(
  //     "nulls.snappy.parquet",
  //     0,
  //     &column_path,
  //     &vec![],
  //     &vec![],
  //     &vec![],
  //   );
  // }

  #[test]
  fn test_triplet_null_column() {
    let path = vec!["b_struct", "b_c_int"];
    let values = vec![
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
    ];
    let def_levels = vec![1, 1, 1, 1, 1, 1, 1, 1];
    let rep_levels = vec![0, 0, 0, 0, 0, 0, 0, 0];
    test_triplet_iter(
      "nulls.snappy.parquet",
      path,
      &values,
      &def_levels,
      &rep_levels,
    );
  }

  #[test]
  fn test_triplet_required_column() {
    let path = vec!["ID"];
    let values = vec![Value::I64(8)];
    let def_levels = vec![0];
    let rep_levels = vec![0];
    test_triplet_iter(
      "nonnullable.impala.parquet",
      path,
      &values,
      &def_levels,
      &rep_levels,
    );
  }

  #[test]
  fn test_triplet_optional_column() {
    let path = vec!["nested_struct", "A"];
    let values = vec![
      Value::Option(Box::new(Some(Value::I32(1)))),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      // Value::Option(Box::new(None)),
      Value::Option(Box::new(Some(Value::I32(7)))),
    ];
    let def_levels = vec![2, 1, 1, 1, 1, 0, 2];
    let rep_levels = vec![0, 0, 0, 0, 0, 0, 0];
    test_triplet_iter(
      "nullable.impala.parquet",
      path,
      &values,
      &def_levels,
      &rep_levels,
    );
  }

  #[test]
  fn test_triplet_optional_list_column() {
    let path = vec!["a", "list", "element", "list", "element", "list", "element"];
    let values = vec![
      Value::Option(Box::new(Some(Value::String("a".to_string())))),
      Value::Option(Box::new(Some(Value::String("b".to_string())))),
      Value::Option(Box::new(Some(Value::String("c".to_string())))),
      // Value::Option(Box::new(None)),
      Value::Option(Box::new(Some(Value::String("d".to_string())))),
      Value::Option(Box::new(Some(Value::String("a".to_string())))),
      Value::Option(Box::new(Some(Value::String("b".to_string())))),
      Value::Option(Box::new(Some(Value::String("c".to_string())))),
      Value::Option(Box::new(Some(Value::String("d".to_string())))),
      // Value::Option(Box::new(None)),
      Value::Option(Box::new(Some(Value::String("e".to_string())))),
      Value::Option(Box::new(Some(Value::String("a".to_string())))),
      Value::Option(Box::new(Some(Value::String("b".to_string())))),
      Value::Option(Box::new(Some(Value::String("c".to_string())))),
      Value::Option(Box::new(Some(Value::String("d".to_string())))),
      Value::Option(Box::new(Some(Value::String("e".to_string())))),
      // Value::Option(Box::new(None)),
      Value::Option(Box::new(Some(Value::String("f".to_string())))),
    ];
    let def_levels = vec![7, 7, 7, 4, 7, 7, 7, 7, 7, 4, 7, 7, 7, 7, 7, 7, 4, 7];
    let rep_levels = vec![0, 3, 2, 1, 2, 0, 3, 2, 3, 1, 2, 0, 3, 2, 3, 2, 1, 2];
    test_triplet_iter(
      "nested_lists.snappy.parquet",
      path,
      &values,
      &def_levels,
      &rep_levels,
    );
  }

  #[test]
  fn test_triplet_optional_map_column() {
    let path = vec!["a", "key_value", "value", "key_value", "key"];
    let values = vec![
      Value::I32(1),
      Value::I32(2),
      Value::I32(1),
      Value::I32(1),
      Value::I32(3),
      Value::I32(4),
      Value::I32(5),
    ];
    let def_levels = vec![4, 4, 4, 2, 3, 4, 4, 4, 4];
    let rep_levels = vec![0, 2, 0, 0, 0, 0, 0, 2, 2];
    test_triplet_iter(
      "nested_maps.snappy.parquet",
      path,
      &values,
      &def_levels,
      &rep_levels,
    );
  }

  // Check triplet iterator across different batch sizes
  fn test_triplet_iter(
    file_name: &str,
    column_path: Vec<&str>,
    expected_values: &[Value],
    expected_def_levels: &[i16],
    expected_rep_levels: &[i16],
  )
  {
    // Convert path into column path
    let path: Vec<String> = column_path.iter().map(|x| x.to_string()).collect();
    let column_path = ColumnPath::from(path);

    let batch_sizes = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 128, 256];
    for batch_size in batch_sizes {
      test_column_in_file(
        file_name,
        batch_size,
        &column_path,
        expected_values,
        expected_def_levels,
        expected_rep_levels,
      );
    }
  }

  // Check values of a selectd column in a file
  fn test_column_in_file(
    file_name: &str,
    batch_size: usize,
    column_path: &ColumnPath,
    expected_values: &[Value],
    expected_def_levels: &[i16],
    expected_rep_levels: &[i16],
  )
  {
    let file = get_test_file(file_name);
    let file_reader = SerializedFileReader::new(file).unwrap();
    // Get schema descriptor
    let file_metadata = file_reader.metadata().file_metadata();
    let schema = file_metadata.schema_descr();
    // Get first row group
    let row_group_reader = file_reader.get_row_group(0).unwrap();

    for i in 0..schema.num_columns() {
      let descr = schema.column(i);
      if descr.path() == column_path {
        let reader = row_group_reader.get_column_reader(i).unwrap();
        test_triplet_column(
          descr,
          reader,
          batch_size,
          expected_values,
          expected_def_levels,
          expected_rep_levels,
        );
      }
    }
  }

  // Check values for individual triplet iterator
  fn test_triplet_column(
    descr: ColumnDescPtr,
    reader: ColumnReader,
    batch_size: usize,
    expected_values: &[Value],
    expected_def_levels: &[i16],
    expected_rep_levels: &[i16],
  )
  {
    let mut iter = TripletIter::new(descr.clone(), reader, batch_size);
    let mut values: Vec<Value> = Vec::new();
    let mut def_levels: Vec<i16> = Vec::new();
    let mut rep_levels: Vec<i16> = Vec::new();

    // assert_eq!(iter.max_def_level(), descr.max_def_level());
    // assert_eq!(iter.max_rep_level(), descr.max_rep_level());

    iter.advance_columns().unwrap();
    while iter.has_next() {
      def_levels.push(iter.current_def_level());
      rep_levels.push(iter.current_rep_level());
      if iter.current_def_level() >= descr.max_def_level() {
        values.push(iter.read().unwrap());
      } else {
        iter.advance_columns().unwrap();
      }
    }

    assert_eq!(
      (&*values, &*def_levels, &*rep_levels),
      (expected_values, expected_def_levels, expected_rep_levels)
    );
  }
}

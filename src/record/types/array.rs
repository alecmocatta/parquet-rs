use basic::{LogicalType, Repetition, Type as PhysicalType};
use column::reader::ColumnReader;
use data_type::{ByteArrayType, FixedLenByteArrayType};
use errors::ParquetError;
use record::{
  reader::{ByteArrayReader, FixedLenByteArrayReader, MapReader},
  schemas::{ArraySchema, StringSchema, VecSchema},
  triplet::TypedTripletIter,
  types::{downcast, Value, DEFAULT_BATCH_SIZE},
  Deserialize,
};
use schema::types::{ColumnDescPtr, ColumnPath, Type};
use std::{collections::HashMap, marker::PhantomData, string::FromUtf8Error};

impl Deserialize for Vec<u8> {
  type Reader = ByteArrayReader;
  type Schema = VecSchema;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    Value::parse(schema).and_then(downcast)
  }

  fn reader(
    _schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    let col_path = ColumnPath::new(path.to_vec());
    let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
    assert_eq!(
      (curr_def_level, curr_rep_level),
      (col_descr.max_def_level(), col_descr.max_rep_level())
    );
    ByteArrayReader {
      column: TypedTripletIter::<ByteArrayType>::new(
        curr_def_level,
        curr_rep_level,
        DEFAULT_BATCH_SIZE,
        col_reader,
      ),
    }
  }
}
impl Deserialize for String {
  // existential type Reader: Reader<Item = Self>;
  type Reader = MapReader<ByteArrayReader, fn(Vec<u8>) -> Result<Self, ParquetError>>;
  type Schema = StringSchema;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    Value::parse(schema).and_then(downcast)
  }

  fn reader(
    _schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    let col_path = ColumnPath::new(path.to_vec());
    let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
    assert_eq!(
      (curr_def_level, curr_rep_level),
      (col_descr.max_def_level(), col_descr.max_rep_level())
    );
    MapReader(
      ByteArrayReader {
        column: TypedTripletIter::<ByteArrayType>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      (|x| {
        String::from_utf8(x)
          .map_err(|err: FromUtf8Error| ParquetError::General(err.to_string()))
      }) as fn(_) -> _,
    )
  }
}

macro_rules! impl_parquet_deserialize_array {
  ($i:tt) => {
    impl Deserialize for [u8; $i] {
      // existential type Reader: Reader<Item = Self>;
      type Reader =
        MapReader<FixedLenByteArrayReader, fn(Vec<u8>) -> Result<Self, ParquetError>>;
      type Schema = ArraySchema<Self>;

      fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        if schema.is_primitive()
          && schema.get_basic_info().repetition() == Repetition::REQUIRED
          && schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY
          && schema.get_basic_info().logical_type() == LogicalType::NONE
          && schema.get_type_length() == $i
        {
          return Ok((schema.name().to_owned(), ArraySchema(PhantomData)));
        }
        Err(ParquetError::General(format!(
          "Can't parse array {:?}",
          schema
        )))
      }

      fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
      ) -> Self::Reader
      {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
          (curr_def_level, curr_rep_level),
          (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        MapReader(
          FixedLenByteArrayReader {
            column: TypedTripletIter::<FixedLenByteArrayType>::new(
              curr_def_level,
              curr_rep_level,
              DEFAULT_BATCH_SIZE,
              col_reader,
            ),
          },
          (|bytes: Vec<_>| {
            let mut ret = std::mem::MaybeUninit::<Self>::uninitialized();
            assert_eq!(bytes.len(), unsafe { ret.get_ref().len() });
            unsafe {
              std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                ret.get_mut().as_mut_ptr(),
                bytes.len(),
              )
            };
            Ok(unsafe { ret.into_inner() })
          }) as fn(_) -> _,
        )
      }
    }
    impl Deserialize for Box<[u8; $i]> {
      // existential type Reader: Reader<Item = Self>;
      type Reader =
        MapReader<FixedLenByteArrayReader, fn(Vec<u8>) -> Result<Self, ParquetError>>;
      type Schema = ArraySchema<[u8; $i]>;

      fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        <[u8; $i]>::parse(schema)
      }

      fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
      ) -> Self::Reader
      {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
          (curr_def_level, curr_rep_level),
          (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        MapReader(
          FixedLenByteArrayReader {
            column: TypedTripletIter::<FixedLenByteArrayType>::new(
              curr_def_level,
              curr_rep_level,
              DEFAULT_BATCH_SIZE,
              col_reader,
            ),
          },
          (|bytes: Vec<_>| {
            let mut ret = box [0u8; $i];
            assert_eq!(bytes.len(), ret.len());
            unsafe {
              std::ptr::copy_nonoverlapping(bytes.as_ptr(), ret.as_mut_ptr(), bytes.len())
            };
            Ok(ret)
          }) as fn(_) -> _,
        )
      }
    }
  };
}

// Implemented on common array lengths, copied from arrayvec
impl_parquet_deserialize_array!(0);
impl_parquet_deserialize_array!(1);
impl_parquet_deserialize_array!(2);
impl_parquet_deserialize_array!(3);
impl_parquet_deserialize_array!(4);
impl_parquet_deserialize_array!(5);
impl_parquet_deserialize_array!(6);
impl_parquet_deserialize_array!(7);
impl_parquet_deserialize_array!(8);
impl_parquet_deserialize_array!(9);
impl_parquet_deserialize_array!(10);
impl_parquet_deserialize_array!(11);
impl_parquet_deserialize_array!(12);
impl_parquet_deserialize_array!(13);
impl_parquet_deserialize_array!(14);
impl_parquet_deserialize_array!(15);
impl_parquet_deserialize_array!(16);
impl_parquet_deserialize_array!(17);
impl_parquet_deserialize_array!(18);
impl_parquet_deserialize_array!(19);
impl_parquet_deserialize_array!(20);
impl_parquet_deserialize_array!(21);
impl_parquet_deserialize_array!(22);
impl_parquet_deserialize_array!(23);
impl_parquet_deserialize_array!(24);
impl_parquet_deserialize_array!(25);
impl_parquet_deserialize_array!(26);
impl_parquet_deserialize_array!(27);
impl_parquet_deserialize_array!(28);
impl_parquet_deserialize_array!(29);
impl_parquet_deserialize_array!(30);
impl_parquet_deserialize_array!(31);
impl_parquet_deserialize_array!(32);
impl_parquet_deserialize_array!(40);
impl_parquet_deserialize_array!(48);
impl_parquet_deserialize_array!(50);
impl_parquet_deserialize_array!(56);
impl_parquet_deserialize_array!(64);
impl_parquet_deserialize_array!(72);
impl_parquet_deserialize_array!(96);
impl_parquet_deserialize_array!(100);
impl_parquet_deserialize_array!(128);
impl_parquet_deserialize_array!(160);
impl_parquet_deserialize_array!(192);
impl_parquet_deserialize_array!(200);
impl_parquet_deserialize_array!(224);
impl_parquet_deserialize_array!(256);
impl_parquet_deserialize_array!(384);
impl_parquet_deserialize_array!(512);
impl_parquet_deserialize_array!(768);
impl_parquet_deserialize_array!(1024);
impl_parquet_deserialize_array!(2048);
impl_parquet_deserialize_array!(4096);
impl_parquet_deserialize_array!(8192);
impl_parquet_deserialize_array!(16384);
impl_parquet_deserialize_array!(32768);
impl_parquet_deserialize_array!(65536);

use super::{
  reader::{
    BoolReader, ByteArrayReader, F32Reader, F64Reader, FixedLenByteArrayReader,
    GroupReader, I32Reader, I64Reader, I96Reader, KeyValueReader, MapReader,
    OptionReader, Reader, RepeatedReader, RootReader, TryIntoReader, TupleReader,
    ValueReader,
  },
  schemas::{
    ArraySchema, BoolSchema, F32Schema, F64Schema, GroupSchema, I16Schema, I32Schema,
    I64Schema, I8Schema, ListSchema, ListSchemaType, MapSchema, OptionSchema, RootSchema,
    StringSchema, TimestampSchema, TupleSchema, U16Schema, U32Schema, U64Schema,
    U8Schema, ValueSchema, VecSchema,
  },
  triplet::TypedTripletIter,
  Deserialize, DisplayType,
};
use crate::{
  basic::{LogicalType, Repetition, Type as PhysicalType},
  column::reader::ColumnReader,
  data_type::{
    BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type,
    Int64Type, Int96, Int96Type,
  },
  errors::ParquetError,
  schema::{
    parser::parse_message_type,
    types::{ColumnDescPtr, ColumnPath, Type},
  },
};
use std::{
  collections::HashMap,
  convert::TryInto,
  error::Error,
  fmt::{self, Debug, Display},
  hash::{Hash, Hasher},
  marker::PhantomData,
  num::TryFromIntError,
  ops::Index,
  rc::Rc,
  slice::SliceIndex,
  str,
  string::FromUtf8Error,
};

/// Default batch size for a reader
const DEFAULT_BATCH_SIZE: usize = 1024;

pub trait Downcast<T> {
  fn downcast(self) -> Result<T, ParquetError>;
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Root<T>(pub T);

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
  Bool(bool),
  U8(u8),
  I8(i8),
  U16(u16),
  I16(i16),
  U32(u32),
  I32(i32),
  U64(u64),
  I64(i64),
  F32(f32),
  F64(f64),
  Timestamp(Timestamp),
  Array(Vec<u8>),
  String(String),
  List(List<Value>),
  Map(Map<Value, Value>),
  Group(Group),
  Option(Box<Option<Value>>),
}
impl Hash for Value {
  fn hash<H: Hasher>(&self, state: &mut H) {
    match self {
      Value::Bool(value) => {
        0u8.hash(state);
        value.hash(state);
      },
      Value::U8(value) => {
        1u8.hash(state);
        value.hash(state);
      },
      Value::I8(value) => {
        2u8.hash(state);
        value.hash(state);
      },
      Value::U16(value) => {
        3u8.hash(state);
        value.hash(state);
      },
      Value::I16(value) => {
        4u8.hash(state);
        value.hash(state);
      },
      Value::U32(value) => {
        5u8.hash(state);
        value.hash(state);
      },
      Value::I32(value) => {
        6u8.hash(state);
        value.hash(state);
      },
      Value::U64(value) => {
        7u8.hash(state);
        value.hash(state);
      },
      Value::I64(value) => {
        8u8.hash(state);
        value.hash(state);
      },
      Value::F32(_value) => {
        9u8.hash(state);
      },
      Value::F64(_value) => {
        10u8.hash(state);
      },
      Value::Timestamp(value) => {
        11u8.hash(state);
        value.hash(state);
      },
      Value::Array(value) => {
        12u8.hash(state);
        value.hash(state);
      },
      Value::String(value) => {
        13u8.hash(state);
        value.hash(state);
      },
      Value::List(value) => {
        14u8.hash(state);
        value.hash(state);
      },
      Value::Map(_value) => {
        15u8.hash(state);
      },
      Value::Group(_value) => {
        16u8.hash(state);
      },
      Value::Option(value) => {
        17u8.hash(state);
        value.hash(state);
      },
    }
  }
}
impl Eq for Value {}

impl Value {
  pub fn is_bool(&self) -> bool {
    if let Value::Bool(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_bool(self) -> Result<bool, ParquetError> {
    if let Value::Bool(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as bool",
        self
      )))
    }
  }

  pub fn is_u8(&self) -> bool {
    if let Value::U8(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u8(self) -> Result<u8, ParquetError> {
    if let Value::U8(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as u8",
        self
      )))
    }
  }

  pub fn is_i8(&self) -> bool {
    if let Value::I8(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i8(self) -> Result<i8, ParquetError> {
    if let Value::I8(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as i8",
        self
      )))
    }
  }

  pub fn is_u16(&self) -> bool {
    if let Value::U16(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u16(self) -> Result<u16, ParquetError> {
    if let Value::U16(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as u16",
        self
      )))
    }
  }

  pub fn is_i16(&self) -> bool {
    if let Value::I16(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i16(self) -> Result<i16, ParquetError> {
    if let Value::I16(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as i16",
        self
      )))
    }
  }

  pub fn is_u32(&self) -> bool {
    if let Value::U32(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u32(self) -> Result<u32, ParquetError> {
    if let Value::U32(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as u32",
        self
      )))
    }
  }

  pub fn is_i32(&self) -> bool {
    if let Value::I32(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i32(self) -> Result<i32, ParquetError> {
    if let Value::I32(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as i32",
        self
      )))
    }
  }

  pub fn is_u64(&self) -> bool {
    if let Value::U64(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u64(self) -> Result<u64, ParquetError> {
    if let Value::U64(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as u64",
        self
      )))
    }
  }

  pub fn is_i64(&self) -> bool {
    if let Value::I64(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i64(self) -> Result<i64, ParquetError> {
    if let Value::I64(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as i64",
        self
      )))
    }
  }

  pub fn is_f32(&self) -> bool {
    if let Value::F32(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_f32(self) -> Result<f32, ParquetError> {
    if let Value::F32(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as f32",
        self
      )))
    }
  }

  pub fn is_f64(&self) -> bool {
    if let Value::F64(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_f64(self) -> Result<f64, ParquetError> {
    if let Value::F64(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as f64",
        self
      )))
    }
  }

  pub fn is_timestamp(&self) -> bool {
    if let Value::Timestamp(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_timestamp(self) -> Result<Timestamp, ParquetError> {
    if let Value::Timestamp(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as timestamp",
        self
      )))
    }
  }

  pub fn is_array(&self) -> bool {
    if let Value::Array(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_array(self) -> Result<Vec<u8>, ParquetError> {
    if let Value::Array(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as array",
        self
      )))
    }
  }

  pub fn is_string(&self) -> bool {
    if let Value::String(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_string(self) -> Result<String, ParquetError> {
    if let Value::String(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as string",
        self
      )))
    }
  }

  pub fn is_list(&self) -> bool {
    if let Value::List(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_list(self) -> Result<List<Value>, ParquetError> {
    if let Value::List(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as list",
        self
      )))
    }
  }

  pub fn is_map(&self) -> bool {
    if let Value::Map(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_map(self) -> Result<Map<Value, Value>, ParquetError> {
    if let Value::Map(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as map",
        self
      )))
    }
  }

  pub fn is_group(&self) -> bool {
    if let Value::Group(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_group(self) -> Result<Group, ParquetError> {
    if let Value::Group(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as group",
        self
      )))
    }
  }

  pub fn is_option(&self) -> bool {
    if let Value::Option(_) = self {
      true
    } else {
      false
    }
  }

  pub fn as_option(self) -> Result<Option<Value>, ParquetError> {
    if let Value::Option(ret) = self {
      Ok(*ret)
    } else {
      Err(ParquetError::General(format!(
        "Cannot access {:?} as option",
        self
      )))
    }
  }
}

impl Downcast<Value> for Value {
  fn downcast(self) -> Result<Value, ParquetError> { Ok(self) }
}
impl Downcast<bool> for Value {
  fn downcast(self) -> Result<bool, ParquetError> { self.as_bool() }
}
impl Downcast<u8> for Value {
  fn downcast(self) -> Result<u8, ParquetError> { self.as_u8() }
}
impl Downcast<i8> for Value {
  fn downcast(self) -> Result<i8, ParquetError> { self.as_i8() }
}
impl Downcast<u16> for Value {
  fn downcast(self) -> Result<u16, ParquetError> { self.as_u16() }
}
impl Downcast<i16> for Value {
  fn downcast(self) -> Result<i16, ParquetError> { self.as_i16() }
}
impl Downcast<u32> for Value {
  fn downcast(self) -> Result<u32, ParquetError> { self.as_u32() }
}
impl Downcast<i32> for Value {
  fn downcast(self) -> Result<i32, ParquetError> { self.as_i32() }
}
impl Downcast<u64> for Value {
  fn downcast(self) -> Result<u64, ParquetError> { self.as_u64() }
}
impl Downcast<i64> for Value {
  fn downcast(self) -> Result<i64, ParquetError> { self.as_i64() }
}
impl Downcast<f32> for Value {
  fn downcast(self) -> Result<f32, ParquetError> { self.as_f32() }
}
impl Downcast<f64> for Value {
  fn downcast(self) -> Result<f64, ParquetError> { self.as_f64() }
}
impl Downcast<Timestamp> for Value {
  fn downcast(self) -> Result<Timestamp, ParquetError> { self.as_timestamp() }
}
impl Downcast<Vec<u8>> for Value {
  fn downcast(self) -> Result<Vec<u8>, ParquetError> { self.as_array() }
}
impl Downcast<String> for Value {
  fn downcast(self) -> Result<String, ParquetError> { self.as_string() }
}
impl<T> Downcast<List<T>> for Value
where Value: Downcast<T>
{
  default fn downcast(self) -> Result<List<T>, ParquetError> {
    let ret = self.as_list()?;
    ret
      .0
      .into_iter()
      .map(Downcast::downcast)
      .collect::<Result<Vec<_>, _>>()
      .map(List)
  }
}
impl Downcast<List<Value>> for Value {
  fn downcast(self) -> Result<List<Value>, ParquetError> { self.as_list() }
}
impl<K, V> Downcast<Map<K, V>> for Value
where
  Value: Downcast<K> + Downcast<V>,
  K: Hash + Eq,
{
  default fn downcast(self) -> Result<Map<K, V>, ParquetError> {
    let ret = self.as_map()?;
    ret
      .0
      .into_iter()
      .map(|(k, v)| Ok((k.downcast()?, v.downcast()?)))
      .collect::<Result<HashMap<_, _>, _>>()
      .map(Map)
  }
}
impl Downcast<Map<Value, Value>> for Value {
  fn downcast(self) -> Result<Map<Value, Value>, ParquetError> { self.as_map() }
}
impl Downcast<Group> for Value {
  fn downcast(self) -> Result<Group, ParquetError> { self.as_group() }
}
impl<T> Downcast<Option<T>> for Value
where Value: Downcast<T>
{
  default fn downcast(self) -> Result<Option<T>, ParquetError> {
    let ret = self.as_option()?;
    match ret {
      Some(t) => Downcast::<T>::downcast(t).map(Some),
      None => Ok(None),
    }
  }
}
impl Downcast<Option<Value>> for Value {
  fn downcast(self) -> Result<Option<Value>, ParquetError> { self.as_option() }
}

impl Deserialize for Value {
  type Reader = ValueReader;
  type Schema = ValueSchema;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    let mut value = None;
    if schema.is_primitive() {
      value = Some(
        match (
          schema.get_physical_type(),
          schema.get_basic_info().logical_type(),
        ) {
          // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
          (PhysicalType::BOOLEAN, LogicalType::NONE) => ValueSchema::Bool(BoolSchema),
          (PhysicalType::INT32, LogicalType::UINT_8) => ValueSchema::U8(U8Schema),
          (PhysicalType::INT32, LogicalType::INT_8) => ValueSchema::I8(I8Schema),
          (PhysicalType::INT32, LogicalType::UINT_16) => ValueSchema::U16(U16Schema),
          (PhysicalType::INT32, LogicalType::INT_16) => ValueSchema::I16(I16Schema),
          (PhysicalType::INT32, LogicalType::UINT_32) => ValueSchema::U32(U32Schema),
          (PhysicalType::INT32, LogicalType::INT_32)
          | (PhysicalType::INT32, LogicalType::NONE) => ValueSchema::I32(I32Schema),
          (PhysicalType::INT32, LogicalType::DATE) => unimplemented!(),
          (PhysicalType::INT32, LogicalType::TIME_MILLIS) => unimplemented!(),
          (PhysicalType::INT32, LogicalType::DECIMAL) => unimplemented!(),
          (PhysicalType::INT64, LogicalType::UINT_64) => ValueSchema::U64(U64Schema),
          (PhysicalType::INT64, LogicalType::INT_64)
          | (PhysicalType::INT64, LogicalType::NONE) => ValueSchema::I64(I64Schema),
          (PhysicalType::INT64, LogicalType::TIME_MICROS) => unimplemented!(),
          // (PhysicalType::INT64,LogicalType::TIME_NANOS) => unimplemented!(),
          (PhysicalType::INT64, LogicalType::TIMESTAMP_MILLIS) => {
            ValueSchema::Timestamp(TimestampSchema::Millis)
          },
          (PhysicalType::INT64, LogicalType::TIMESTAMP_MICROS) => {
            ValueSchema::Timestamp(TimestampSchema::Micros)
          },
          // (PhysicalType::INT64,LogicalType::TIMESTAMP_NANOS) => unimplemented!(),
          (PhysicalType::INT64, LogicalType::DECIMAL) => unimplemented!(),
          (PhysicalType::INT96, LogicalType::NONE) => {
            ValueSchema::Timestamp(TimestampSchema::Int96)
          },
          (PhysicalType::FLOAT, LogicalType::NONE) => ValueSchema::F32(F32Schema),
          (PhysicalType::DOUBLE, LogicalType::NONE) => ValueSchema::F64(F64Schema),
          (PhysicalType::BYTE_ARRAY, LogicalType::UTF8)
          | (PhysicalType::BYTE_ARRAY, LogicalType::ENUM)
          | (PhysicalType::BYTE_ARRAY, LogicalType::JSON)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::UTF8)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::ENUM)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::JSON) => {
            ValueSchema::String(StringSchema)
          },
          (PhysicalType::BYTE_ARRAY, LogicalType::NONE)
          | (PhysicalType::BYTE_ARRAY, LogicalType::BSON)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::BSON) => {
            ValueSchema::Array(VecSchema(
              if schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY {
                Some(schema.get_type_length().try_into().unwrap())
              } else {
                None
              },
            ))
          },
          (PhysicalType::BYTE_ARRAY, LogicalType::DECIMAL)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL) => {
            unimplemented!()
          },
          (PhysicalType::BYTE_ARRAY, LogicalType::INTERVAL)
          | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL) => {
            unimplemented!()
          },
          (physical_type, logical_type) => {
            return Err(ParquetError::General(format!(
              "Can't parse primitive ({:?}, {:?})",
              physical_type, logical_type
            )));
          },
        },
      );
    }
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
    if value.is_none() && !schema.is_schema() {
      value = parse_list::<Value>(schema)
        .ok()
        .map(|value| ValueSchema::List(Box::new(value)));
    }
    if value.is_none() && !schema.is_schema() {
      value = parse_map::<Value, Value>(schema)
        .ok()
        .map(|value| ValueSchema::Map(Box::new(value)));
    }

    if value.is_none() && schema.is_group() && !schema.is_schema() {
      let mut lookup = HashMap::new();
      value = Some(ValueSchema::Group(GroupSchema(
        schema
          .get_fields()
          .iter()
          .map(|schema| {
            Value::parse(&*schema).map(|(name, schema)| {
              let x = lookup.insert(name, lookup.len());
              assert!(x.is_none());
              schema
            })
          })
          .collect::<Result<Vec<_>, _>>()?,
        lookup,
      )));
    }

    let mut value = value.ok_or(ParquetError::General(format!(
      "Can't parse group {:?}",
      schema
    )))?;

    match schema.get_basic_info().repetition() {
      Repetition::OPTIONAL => {
        value = ValueSchema::Option(Box::new(OptionSchema(value)));
      },
      Repetition::REPEATED => {
        value = ValueSchema::List(Box::new(ListSchema(value, ListSchemaType::Repeated)));
      },
      Repetition::REQUIRED => (),
    }

    Ok((schema.name().to_owned(), value))
  }

  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    match *schema {
      ValueSchema::Bool(ref schema) => ValueReader::Bool(<bool as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::U8(ref schema) => ValueReader::U8(<u8 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::I8(ref schema) => ValueReader::I8(<i8 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::U16(ref schema) => ValueReader::U16(<u16 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::I16(ref schema) => ValueReader::I16(<i16 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::U32(ref schema) => ValueReader::U32(<u32 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::I32(ref schema) => ValueReader::I32(<i32 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::U64(ref schema) => ValueReader::U64(<u64 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::I64(ref schema) => ValueReader::I64(<i64 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::F32(ref schema) => ValueReader::F32(<f32 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::F64(ref schema) => ValueReader::F64(<f64 as Deserialize>::reader(
        schema,
        path,
        curr_def_level,
        curr_rep_level,
        paths,
      )),
      ValueSchema::Timestamp(ref schema) => {
        ValueReader::Timestamp(<Timestamp as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        ))
      },
      ValueSchema::Array(ref schema) => {
        ValueReader::Array(<Vec<u8> as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        ))
      },
      ValueSchema::String(ref schema) => {
        ValueReader::String(<String as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        ))
      },
      ValueSchema::List(ref schema) => {
        ValueReader::List(Box::new(<List<Value> as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        )))
      },
      ValueSchema::Map(ref schema) => {
        ValueReader::Map(Box::new(<Map<Value, Value> as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        )))
      },
      ValueSchema::Group(ref schema) => {
        ValueReader::Group(<Group as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        ))
      },
      ValueSchema::Option(ref schema) => {
        ValueReader::Option(Box::new(<Option<Value> as Deserialize>::reader(
          schema,
          path,
          curr_def_level,
          curr_rep_level,
          paths,
        )))
      },
    }
  }
}

#[derive(Clone, PartialEq)]
pub struct Group(pub(crate) Vec<Value>, pub(crate) Rc<HashMap<String, usize>>);
pub type Row = Group;

impl Deserialize for Group {
  type Reader = GroupReader;
  type Schema = GroupSchema;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    if schema.is_group()
      && !schema.is_schema()
      && schema.get_basic_info().repetition() == Repetition::REQUIRED
    {
      let mut map = HashMap::new();
      let fields = schema
        .get_fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
          let (name, schema) = <Value as Deserialize>::parse(&**field)?;
          let x = map.insert(name, i);
          assert!(x.is_none());
          Ok(schema)
        })
        .collect::<Result<Vec<ValueSchema>, ParquetError>>()?;
      let schema_ = GroupSchema(fields, map);
      return Ok((schema.name().to_owned(), schema_));
    }
    Err(ParquetError::General(format!(
      "Struct {}",
      stringify!($struct)
    )))
  }

  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    let mut names_ = vec![None; schema.0.len()];
    for (name, &index) in schema.1.iter() {
      names_[index].replace(name.to_owned());
    }
    let readers = schema
      .0
      .iter()
      .enumerate()
      .map(|(i, field)| {
        path.push(names_[i].take().unwrap());
        let ret = Value::reader(field, path, curr_def_level, curr_rep_level, paths);
        path.pop().unwrap();
        ret
      })
      .collect();
    GroupReader {
      def_level: curr_def_level,
      readers,
      fields: Rc::new(schema.1.clone()),
    }
  }
}
impl Deserialize for Root<Group> {
  type Reader = RootReader<GroupReader>;
  type Schema = RootSchema<Group, GroupSchema>;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    if schema.is_schema() {
      let mut map = HashMap::new();
      let fields = schema
        .get_fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
          let (name, schema) = <Value as Deserialize>::parse(&**field)?;
          let x = map.insert(name, i);
          assert!(x.is_none());
          Ok(schema)
        })
        .collect::<Result<Vec<ValueSchema>, ParquetError>>()?;
      let schema_ = GroupSchema(fields, map);
      return Ok((
        String::from(""),
        RootSchema(schema.name().to_owned(), schema_, PhantomData),
      ));
    }
    Err(ParquetError::General(format!(
      "Struct {}",
      stringify!($struct)
    )))
  }

  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    RootReader(Group::reader(
      &schema.1,
      path,
      curr_def_level,
      curr_rep_level,
      paths,
    ))
  }
}

impl Group {
  pub fn get(&self, k: &str) -> Option<&Value> {
    self.1.get(k).map(|&offset| &self.0[offset])
  }
}
impl<I> Index<I> for Group
where I: SliceIndex<[Value]>
{
  type Output = <I as SliceIndex<[Value]>>::Output;

  fn index(&self, index: I) -> &Self::Output { self.0.index(index) }
}
impl Debug for Group {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    let mut printer = f.debug_struct("Group");
    let fields = self.0.iter();
    let mut names = vec![None; self.1.len()];
    for (name, &index) in self.1.iter() {
      names[index].replace(name);
    }
    let names = names.into_iter().map(Option::unwrap);
    for (name, field) in names.zip(fields) {
      printer.field(name, field);
    }
    printer.finish()
  }
}
impl From<HashMap<String, Value>> for Group {
  fn from(hashmap: HashMap<String, Value>) -> Self {
    let mut keys = HashMap::new();
    Group(
      hashmap
        .into_iter()
        .map(|(key, value)| {
          keys.insert(key, keys.len());
          value
        })
        .collect(),
      Rc::new(keys),
    )
  }
}
impl Into<HashMap<String, Value>> for Group {
  fn into(self) -> HashMap<String, Value> {
    let fields = self.0.into_iter();
    let mut names = vec![None; self.1.len()];
    for (name, &index) in self.1.iter() {
      names[index].replace(name.to_owned());
    }
    let names = names.into_iter().map(Option::unwrap);
    names.zip(fields).collect()
  }
}

// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
fn parse_list<T: Deserialize>(
  schema: &Type,
) -> Result<ListSchema<T::Schema>, ParquetError> {
  if schema.is_group()
    && schema.get_basic_info().logical_type() == LogicalType::LIST
    && schema.get_fields().len() == 1
  {
    let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
    if sub_schema.get_basic_info().repetition() == Repetition::REPEATED {
      return Ok(
        if sub_schema.is_group()
          && sub_schema.get_fields().len() == 1
          && sub_schema.name() != "array"
          && sub_schema.name() != format!("{}_tuple", schema.name())
        {
          let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
          let list_name = if sub_schema.name() == "list" {
            None
          } else {
            Some(sub_schema.name().to_owned())
          };
          let element_name = if element.name() == "element" {
            None
          } else {
            Some(element.name().to_owned())
          };

          ListSchema(
            T::parse(&*element)?.1,
            ListSchemaType::List(list_name, element_name),
          )
        } else {
          let element_name = sub_schema.name().to_owned();
          ListSchema(
            T::parse(&*sub_schema)?.1,
            ListSchemaType::ListCompat(element_name),
          )
        },
      );
    }
  }
  Err(ParquetError::General(String::from(
    "Couldn't parse List<T>",
  )))
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct List<T>(pub(super) Vec<T>);

impl<T> Deserialize for List<T>
where T: Deserialize
{
  // existential type Reader: Reader<Item = Self>;
  type Reader =
    MapReader<RepeatedReader<T::Reader>, fn(Vec<T>) -> Result<Self, ParquetError>>;
  type Schema = ListSchema<T::Schema>;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    // <Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
    //  match schema {
    //    ValueSchema::List(box ListSchema(schema, a)) => Ok((name, ListSchema(schema,
    // a))),    _ => Err(ParquetError::General(String::from(""))),
    //  }
    // })
    if !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED
    {
      return parse_list::<T>(schema).map(|schema2| (schema.name().to_owned(), schema2));
    }
    if schema.get_basic_info().repetition() == Repetition::REPEATED {
      let mut schema2: Type = schema.clone();
      let basic_info = match schema2 {
        Type::PrimitiveType {
          ref mut basic_info, ..
        } => basic_info,
        Type::GroupType {
          ref mut basic_info, ..
        } => basic_info,
      };
      basic_info.set_repetition(Some(Repetition::REQUIRED));
      return Ok((
        schema.name().to_owned(),
        ListSchema(T::parse(&schema2)?.1, ListSchemaType::Repeated),
      ));
    }
    Err(ParquetError::General(String::from(
      "Couldn't parse List<T>",
    )))
  }

  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    MapReader(
      match schema.1 {
        ListSchemaType::List(ref list_name, ref element_name) => {
          let list_name = list_name.as_ref().map(|x| &**x).unwrap_or("list");
          let element_name = element_name.as_ref().map(|x| &**x).unwrap_or("element");

          path.push(list_name.to_owned());
          path.push(element_name.to_owned());
          let reader = T::reader(
            &schema.0,
            path,
            curr_def_level + 1,
            curr_rep_level + 1,
            paths,
          );
          path.pop().unwrap();
          path.pop().unwrap();

          RepeatedReader {
            def_level: curr_def_level,
            rep_level: curr_rep_level,
            reader,
          }
        },
        ListSchemaType::ListCompat(ref element_name) => {
          path.push(element_name.to_owned());
          let reader = T::reader(
            &schema.0,
            path,
            curr_def_level + 1,
            curr_rep_level + 1,
            paths,
          );
          path.pop().unwrap();

          RepeatedReader {
            def_level: curr_def_level,
            rep_level: curr_rep_level,
            reader,
          }
        },
        ListSchemaType::Repeated => {
          let reader = T::reader(
            &schema.0,
            path,
            curr_def_level + 1,
            curr_rep_level + 1,
            paths,
          );
          RepeatedReader {
            def_level: curr_def_level,
            rep_level: curr_rep_level,
            reader,
          }
        },
      },
      (|x| Ok(List(x))) as fn(_) -> _,
    )
  }
}

impl<T> From<Vec<T>> for List<T> {
  fn from(vec: Vec<T>) -> Self { List(vec) }
}
impl<T> Into<Vec<T>> for List<T> {
  fn into(self) -> Vec<T> { self.0 }
}
impl<T, I> Index<I> for List<T>
where I: SliceIndex<[T]>
{
  type Output = <I as SliceIndex<[T]>>::Output;

  fn index(&self, index: I) -> &Self::Output { self.0.index(index) }
}

// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
fn parse_map<K: Deserialize, V: Deserialize>(
  schema: &Type,
) -> Result<MapSchema<K::Schema, V::Schema>, ParquetError> {
  if schema.is_group()
    && (schema.get_basic_info().logical_type() == LogicalType::MAP
      || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE)
    && schema.get_fields().len() == 1
  {
    let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
    if sub_schema.is_group()
      && !sub_schema.is_schema()
      && sub_schema.get_basic_info().repetition() == Repetition::REPEATED
      && sub_schema.get_fields().len() == 2
    {
      let mut fields = sub_schema.get_fields().into_iter();
      let (key, value_) = (fields.next().unwrap(), fields.next().unwrap());
      let key_value_name = if sub_schema.name() == "key_value" {
        None
      } else {
        Some(sub_schema.name().to_owned())
      };
      let key_name = if key.name() == "key" {
        None
      } else {
        Some(key.name().to_owned())
      };
      let value_name = if value_.name() == "value" {
        None
      } else {
        Some(value_.name().to_owned())
      };
      return Ok(MapSchema(
        K::parse(&*key)?.1,
        V::parse(&*value_)?.1,
        key_value_name,
        key_name,
        value_name,
      ));
    }
  }
  Err(ParquetError::General(String::from(
    "Couldn't parse Map<K,V>",
  )))
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Map<K: Hash + Eq, V>(pub(super) HashMap<K, V>);

impl<K, V> Deserialize for Map<K, V>
where
  K: Deserialize + Hash + Eq,
  V: Deserialize,
{
  // existential type Reader: Reader<Item = Self>;
  type Reader = MapReader<
    KeyValueReader<K::Reader, V::Reader>,
    fn(Vec<(K, V)>) -> Result<Self, ParquetError>,
  >;
  type Schema = MapSchema<K::Schema, V::Schema>;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    if !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED
    {
      return parse_map::<K, V>(schema).map(|schema2| (schema.name().to_owned(), schema2));
    }
    Err(ParquetError::General(String::from(
      "Couldn't parse Map<K,V>",
    )))
  }

  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    let key_value_name = schema.2.as_ref().map(|x| &**x).unwrap_or("key_value");
    let key_name = schema.3.as_ref().map(|x| &**x).unwrap_or("key");
    let value_name = schema.4.as_ref().map(|x| &**x).unwrap_or("value");

    path.push(key_value_name.to_owned());
    path.push(key_name.to_owned());
    let keys_reader = K::reader(
      &schema.0,
      path,
      curr_def_level + 1,
      curr_rep_level + 1,
      paths,
    );
    path.pop().unwrap();
    path.push(value_name.to_owned());
    let values_reader = V::reader(
      &schema.1,
      path,
      curr_def_level + 1,
      curr_rep_level + 1,
      paths,
    );
    path.pop().unwrap();
    path.pop().unwrap();

    MapReader(
      KeyValueReader {
        def_level: curr_def_level,
        rep_level: curr_rep_level,
        keys_reader,
        values_reader,
      },
      (|x: Vec<_>| Ok(Map(x.into_iter().collect()))) as fn(_) -> _,
    )
  }
}

impl<K, V> From<HashMap<K, V>> for Map<K, V>
where K: Hash + Eq
{
  fn from(hashmap: HashMap<K, V>) -> Self { Map(hashmap) }
}
impl<K, V> Into<HashMap<K, V>> for Map<K, V>
where K: Hash + Eq
{
  fn into(self) -> HashMap<K, V> { self.0 }
}

impl<T> Deserialize for Option<T>
where T: Deserialize
{
  type Reader = OptionReader<T::Reader>;
  type Schema = OptionSchema<T::Schema>;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    // <Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
    //   Ok((name, OptionSchema(schema.as_option()?.0.downcast()?)))
    // })
    if schema.get_basic_info().repetition() == Repetition::OPTIONAL {
      let mut schema2: Type = schema.clone();
      let basic_info = match schema2 {
        Type::PrimitiveType {
          ref mut basic_info, ..
        } => basic_info,
        Type::GroupType {
          ref mut basic_info, ..
        } => basic_info,
      };
      basic_info.set_repetition(Some(Repetition::REQUIRED));
      return Ok((
        schema.name().to_owned(),
        OptionSchema(T::parse(&schema2)?.1),
      ));
    }
    Err(ParquetError::General(String::from(
      "Couldn't parse Option<T>",
    )))
  }

  fn reader(
    schema: &Self::Schema,
    path: &mut Vec<String>,
    curr_def_level: i16,
    curr_rep_level: i16,
    paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
  ) -> Self::Reader
  {
    OptionReader {
      def_level: curr_def_level,
      reader: <T as Deserialize>::reader(
        &schema.0,
        path,
        curr_def_level + 1,
        curr_rep_level,
        paths,
      ),
    }
  }
}

fn downcast<T>(
  (name, schema): (String, ValueSchema),
) -> Result<(String, T), ParquetError>
where ValueSchema: Downcast<T> {
  schema.downcast().map(|schema| (name, schema))
}

impl Deserialize for bool {
  type Reader = BoolReader;
  type Schema = BoolSchema;

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
    BoolReader {
      column: TypedTripletIter::<BoolType>::new(
        curr_def_level,
        curr_rep_level,
        DEFAULT_BATCH_SIZE,
        col_reader,
      ),
    }
  }
}

impl Deserialize for i8 {
  type Reader = TryIntoReader<I32Reader, i8>;
  type Schema = I8Schema;

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
    TryIntoReader(
      I32Reader {
        column: TypedTripletIter::<Int32Type>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      PhantomData,
    )
  }
}
impl Deserialize for u8 {
  type Reader = TryIntoReader<I32Reader, u8>;
  type Schema = U8Schema;

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
    TryIntoReader(
      I32Reader {
        column: TypedTripletIter::<Int32Type>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      PhantomData,
    )
  }
}

impl Deserialize for i16 {
  type Reader = TryIntoReader<I32Reader, i16>;
  type Schema = I16Schema;

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
    TryIntoReader(
      I32Reader {
        column: TypedTripletIter::<Int32Type>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      PhantomData,
    )
  }
}
impl Deserialize for u16 {
  type Reader = TryIntoReader<I32Reader, u16>;
  type Schema = U16Schema;

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
    TryIntoReader(
      I32Reader {
        column: TypedTripletIter::<Int32Type>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      PhantomData,
    )
  }
}

impl Deserialize for i32 {
  type Reader = I32Reader;
  type Schema = I32Schema;

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
    I32Reader {
      column: TypedTripletIter::<Int32Type>::new(
        curr_def_level,
        curr_rep_level,
        DEFAULT_BATCH_SIZE,
        col_reader,
      ),
    }
  }
}
impl Deserialize for u32 {
  // existential type Reader: Reader<Item = Self>;
  type Reader = MapReader<I32Reader, fn(i32) -> Result<Self, ParquetError>>;
  type Schema = U32Schema;

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
      I32Reader {
        column: TypedTripletIter::<Int32Type>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      (|x| Ok(x as u32)) as fn(_) -> _,
    )
  }
}

impl Deserialize for i64 {
  type Reader = I64Reader;
  type Schema = I64Schema;

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
    I64Reader {
      column: TypedTripletIter::<Int64Type>::new(
        curr_def_level,
        curr_rep_level,
        DEFAULT_BATCH_SIZE,
        col_reader,
      ),
    }
  }
}
impl Deserialize for u64 {
  // existential type Reader: Reader<Item = Self>;
  type Reader = MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>;
  type Schema = U64Schema;

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
      I64Reader {
        column: TypedTripletIter::<Int64Type>::new(
          curr_def_level,
          curr_rep_level,
          DEFAULT_BATCH_SIZE,
          col_reader,
        ),
      },
      (|x| Ok(x as u64)) as fn(_) -> _,
    )
  }
}

impl Deserialize for f32 {
  type Reader = F32Reader;
  type Schema = F32Schema;

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
    F32Reader {
      column: TypedTripletIter::<FloatType>::new(
        curr_def_level,
        curr_rep_level,
        DEFAULT_BATCH_SIZE,
        col_reader,
      ),
    }
  }
}
impl Deserialize for f64 {
  type Reader = F64Reader;
  type Schema = F64Schema;

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
    F64Reader {
      column: TypedTripletIter::<DoubleType>::new(
        curr_def_level,
        curr_rep_level,
        DEFAULT_BATCH_SIZE,
        col_reader,
      ),
    }
  }
}

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const SECONDS_PER_DAY: i64 = 86_400;
const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Timestamp(pub(super) Int96);
impl Timestamp {
  fn as_day_nanos(&self) -> (i64, i64) {
    let day = self.0.data()[2] as i64;
    let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
    (day, nanoseconds)
  }

  fn as_millis(&self) -> Option<i64> {
    let day = self.0.data()[2] as i64;
    let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
    Some(seconds * MILLIS_PER_SECOND + nanoseconds / NANOS_PER_MICRO / MICROS_PER_MILLI)
  }

  fn as_micros(&self) -> Option<i64> {
    let day = self.0.data()[2] as i64;
    let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
    Some(seconds * MILLIS_PER_SECOND * MICROS_PER_MILLI + nanoseconds / NANOS_PER_MICRO)
  }

  fn as_nanos(&self) -> Option<i64> {
    let day = self.0.data()[2] as i64;
    let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
    Some(seconds * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO + nanoseconds)
  }
}

impl Deserialize for Timestamp {
  // existential type Reader: Reader<Item = Self>;
  type Reader = sum::Sum3<
    MapReader<I96Reader, fn(Int96) -> Result<Self, ParquetError>>,
    MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>,
    MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>,
  >;
  type Schema = TimestampSchema;

  fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
    Value::parse(schema).and_then(downcast)
  }

  fn reader(
    schema: &Self::Schema,
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
    match schema {
      TimestampSchema::Int96 => sum::Sum3::A(MapReader(
        I96Reader {
          column: TypedTripletIter::<Int96Type>::new(
            curr_def_level,
            curr_rep_level,
            DEFAULT_BATCH_SIZE,
            col_reader,
          ),
        },
        (|x| Ok(Timestamp(x))) as fn(_) -> _,
      )),
      TimestampSchema::Millis => sum::Sum3::B(MapReader(
        I64Reader {
          column: TypedTripletIter::<Int64Type>::new(
            curr_def_level,
            curr_rep_level,
            DEFAULT_BATCH_SIZE,
            col_reader,
          ),
        },
        (|millis| {
          let day: i64 = ((JULIAN_DAY_OF_EPOCH * SECONDS_PER_DAY * MILLIS_PER_SECOND)
            + millis)
            / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
          let nanoseconds: i64 = (millis
            - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND))
            * MICROS_PER_MILLI
            * NANOS_PER_MICRO;

          Ok(Timestamp(Int96::new(
            (nanoseconds & 0xffff).try_into().unwrap(),
            ((nanoseconds as u64) >> 32).try_into().unwrap(),
            day.try_into().map_err(|err: TryFromIntError| {
              ParquetError::General(err.description().to_owned())
            })?,
          )))
        }) as fn(_) -> _,
      )),
      TimestampSchema::Micros => sum::Sum3::C(MapReader(
        I64Reader {
          column: TypedTripletIter::<Int64Type>::new(
            curr_def_level,
            curr_rep_level,
            DEFAULT_BATCH_SIZE,
            col_reader,
          ),
        },
        (|micros| {
          let day: i64 = ((JULIAN_DAY_OF_EPOCH
            * SECONDS_PER_DAY
            * MILLIS_PER_SECOND
            * MICROS_PER_MILLI)
            + micros)
            / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
          let nanoseconds: i64 = (micros
            - ((day - JULIAN_DAY_OF_EPOCH)
              * SECONDS_PER_DAY
              * MILLIS_PER_SECOND
              * MICROS_PER_MILLI))
            * NANOS_PER_MICRO;

          Ok(Timestamp(Int96::new(
            (nanoseconds & 0xffff).try_into().unwrap(),
            ((nanoseconds as u64) >> 32).try_into().unwrap(),
            day.try_into().map_err(|err: TryFromIntError| {
              ParquetError::General(err.description().to_owned())
            })?,
          )))
        }) as fn(_) -> _,
      )),
    }
  }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Date(pub(super) i32);
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Time(pub(super) i64);

// impl Deserialize for parquet::data_type::Decimal {
//  type Schema = DecimalSchema;
// type Reader = Reader;
// fn placeholder() -> Self::Schema {
//
// }
// fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
//  unimplemented!()
// }
//  fn render(name: &str, schema: &Self::Schema) -> Type {
//    Type::primitive_type_builder(name, PhysicalType::DOUBLE)
//  .with_repetition(Repetition::REQUIRED)
//  .with_logical_type(LogicalType::NONE)
//  .with_length(-1)
//  .with_precision(-1)
//  .with_scale(-1)
//  .build().unwrap()
// Type::PrimitiveType {
//      basic_info: BasicTypeInfo {
//        name: String::from(schema),
//        repetition: Some(Repetition::REQUIRED),
//        logical_type: LogicalType::DECIMAL,
//        id: None,
//      }
//      physical_type: PhysicalType::
//  }
// }
// struct DecimalSchema {
//  scale: u32,
//  precision: u32,
// }

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
        Err(ParquetError::General(String::from("")))
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

macro_rules! impl_parquet_deserialize_tuple {
  ($($t:ident $i:tt)*) => (
    impl<$($t,)*> Reader for TupleReader<($($t,)*)> where $($t: Reader,)* {
      type Item = ($($t::Item,)*);

      fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
        Ok((
          $((self.0).$i.read_field()?,)*
        ))
      }
      fn advance_columns(&mut self) {
        $((self.0).$i.advance_columns();)*
      }
      fn has_next(&self) -> bool {
        // $((self.0).$i.has_next() &&)* true
        $(if true { (self.0).$i.has_next() } else)*
        {
          true
        }
      }
      fn current_def_level(&self) -> i16 {
        $(if true { (self.0).$i.current_def_level() } else)*
        {
          panic!("Current definition level: empty group reader")
        }
      }
      fn current_rep_level(&self) -> i16 {
        $(if true { (self.0).$i.current_rep_level() } else)*
        {
          panic!("Current repetition level: empty group reader")
        }
      }
    }
    // impl<$($t,)*> str::FromStr for RootSchema<($($t,)*),TupleSchema<($((String,$t::Schema,),)*)>> where $($t: Deserialize,)* {
    //   type Err = ParquetError;

    //   fn from_str(s: &str) -> Result<Self, Self::Err> {
    //     parse_message_type(s).and_then(|x|<Root<($($t,)*)> as Deserialize>::parse(&x).map_err(|err| {
    //       // let x: Type = <Root<($($t,)*)> as Deserialize>::render("", &<Root<($($t,)*)> as Deserialize>::placeholder());
    //       let a = Vec::new();
    //       // print_schema(&mut a, &x);
    //       ParquetError::General(format!(
    //         "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
    //         s,
    //         String::from_utf8(a).unwrap(),
    //         err
    //       ))
    //     })).map(|x|x.1)
    //   }
    // }
    impl<$($t,)*> Debug for TupleSchema<($((String,$t,),)*)> where $($t: Debug,)* {
      fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_tuple("TupleSchema")
          $(.field(&(self.0).$i))*
          .finish()
      }
    }
    impl<$($t,)*> Display for TupleSchema<($((String,$t,),)*)> where $($t: Display,)* {
      fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str("TupleSchema")
      }
    }
    impl<$($t,)*> DisplayType for TupleSchema<($((String,$t,),)*)> where $($t: DisplayType,)* {
      fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str("TupleSchema")
      }
    }
    impl<$($t,)*> Deserialize for Root<($($t,)*)> where $($t: Deserialize,)* {
      type Schema = RootSchema<($($t,)*),TupleSchema<($((String,$t::Schema,),)*)>>;
      type Reader = RootReader<TupleReader<($($t::Reader,)*)>>;

      fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
        if schema.is_schema() {
          let mut fields = schema.get_fields().iter();
          let schema_ = RootSchema(schema.name().to_owned(), TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*)), PhantomData);
          if fields.next().is_none() {
            return Ok((String::from(""), schema_))
          }
        }
        Err(ParquetError::General(String::from("")))
      }
      fn reader(schema: &Self::Schema, path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
        RootReader(<($($t,)*) as Deserialize>::reader(&schema.1, path, curr_def_level, curr_rep_level, paths))
      }
    }
    impl<$($t,)*> Deserialize for ($($t,)*) where $($t: Deserialize,)* {
      type Schema = TupleSchema<($((String,$t::Schema,),)*)>;
      type Reader = TupleReader<($($t::Reader,)*)>;

      fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
        if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
          let mut fields = schema.get_fields().iter();
          let schema_ = TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*));
          if fields.next().is_none() {
            return Ok((schema.name().to_owned(), schema_))
          }
        }
        Err(ParquetError::General(String::from("")))
      }
      #[allow(unused_variables)]
      fn reader(schema: &Self::Schema, path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
        $(
          path.push((schema.0).$i.0.to_owned());
          #[allow(non_snake_case)]
          let $t = <$t as Deserialize>::reader(&(schema.0).$i.1, path, curr_def_level, curr_rep_level, paths);
          path.pop().unwrap();
        )*;
        TupleReader(($($t,)*))
      }
    }
    impl<$($t,)*> Downcast<($($t,)*)> for Value where Value: $(Downcast<$t> +)* {
      fn downcast(self) -> Result<($($t,)*),ParquetError> {
        #[allow(unused_mut,unused_variables)]
        let mut fields = self.as_group()?.0.into_iter();
        Ok(($({$i;fields.next().unwrap().downcast()?},)*))
      }
    }
    impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
      fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>,ParquetError> {
        let group = self.as_group()?;
        #[allow(unused_mut,unused_variables)]
        let mut fields = group.0.into_iter();
        let mut names = vec![None; group.1.len()];
        for (name,&index) in group.1.iter() {
          names[index].replace(name.to_owned());
        }
        #[allow(unused_mut,unused_variables)]
        let mut names = names.into_iter().map(Option::unwrap);
        Ok(TupleSchema(($({$i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
      }
    }
  );
}

impl_parquet_deserialize_tuple!();
impl_parquet_deserialize_tuple!(A 0);
impl_parquet_deserialize_tuple!(A 0 B 1);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31 AG 32);

use super::{types::Downcast, DebugType};
use crate::errors::ParquetError;
use std::{
  collections::HashMap,
  fmt::{self, Debug},
  marker::PhantomData,
};

#[derive(Debug)]
pub enum ValueSchema {
  Bool(BoolSchema),
  U8(U8Schema),
  I8(I8Schema),
  U16(U16Schema),
  I16(I16Schema),
  U32(U32Schema),
  I32(I32Schema),
  U64(U64Schema),
  I64(I64Schema),
  F32(F32Schema),
  F64(F64Schema),
  Timestamp(TimestampSchema),
  Array(VecSchema),
  String(StringSchema),
  List(Box<ListSchema<ValueSchema>>),
  Map(Box<MapSchema<ValueSchema, ValueSchema>>),
  Group(GroupSchema),
  Option(Box<OptionSchema<ValueSchema>>),
}
impl DebugType for ValueSchema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("ValueSchema") }
}
impl ValueSchema {
  pub fn is_bool(&self) -> bool {
    if let ValueSchema::Bool(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_bool(self) -> Result<BoolSchema, ParquetError> {
    if let ValueSchema::Bool(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_u8(&self) -> bool {
    if let ValueSchema::U8(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u8(self) -> Result<U8Schema, ParquetError> {
    if let ValueSchema::U8(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_i8(&self) -> bool {
    if let ValueSchema::I8(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i8(self) -> Result<I8Schema, ParquetError> {
    if let ValueSchema::I8(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_u16(&self) -> bool {
    if let ValueSchema::U16(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u16(self) -> Result<U16Schema, ParquetError> {
    if let ValueSchema::U16(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_i16(&self) -> bool {
    if let ValueSchema::I16(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i16(self) -> Result<I16Schema, ParquetError> {
    if let ValueSchema::I16(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_u32(&self) -> bool {
    if let ValueSchema::U32(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u32(self) -> Result<U32Schema, ParquetError> {
    if let ValueSchema::U32(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_i32(&self) -> bool {
    if let ValueSchema::I32(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i32(self) -> Result<I32Schema, ParquetError> {
    if let ValueSchema::I32(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_u64(&self) -> bool {
    if let ValueSchema::U64(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_u64(self) -> Result<U64Schema, ParquetError> {
    if let ValueSchema::U64(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_i64(&self) -> bool {
    if let ValueSchema::I64(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_i64(self) -> Result<I64Schema, ParquetError> {
    if let ValueSchema::I64(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_f32(&self) -> bool {
    if let ValueSchema::F32(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_f32(self) -> Result<F32Schema, ParquetError> {
    if let ValueSchema::F32(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_f64(&self) -> bool {
    if let ValueSchema::F64(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_f64(self) -> Result<F64Schema, ParquetError> {
    if let ValueSchema::F64(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_timestamp(&self) -> bool {
    if let ValueSchema::Timestamp(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_timestamp(self) -> Result<TimestampSchema, ParquetError> {
    if let ValueSchema::Timestamp(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_array(&self) -> bool {
    if let ValueSchema::Array(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_array(self) -> Result<VecSchema, ParquetError> {
    if let ValueSchema::Array(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_string(&self) -> bool {
    if let ValueSchema::String(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_string(self) -> Result<StringSchema, ParquetError> {
    if let ValueSchema::String(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_list(&self) -> bool {
    if let ValueSchema::List(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_list(self) -> Result<ListSchema<ValueSchema>, ParquetError> {
    if let ValueSchema::List(ret) = self {
      Ok(*ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_map(&self) -> bool {
    if let ValueSchema::Map(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_map(self) -> Result<MapSchema<ValueSchema, ValueSchema>, ParquetError> {
    if let ValueSchema::Map(ret) = self {
      Ok(*ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_group(&self) -> bool {
    if let ValueSchema::Group(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_group(self) -> Result<GroupSchema, ParquetError> {
    if let ValueSchema::Group(ret) = self {
      Ok(ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }

  pub fn is_option(&self) -> bool {
    if let ValueSchema::Option(ret) = self {
      true
    } else {
      false
    }
  }

  pub fn as_option(self) -> Result<OptionSchema<ValueSchema>, ParquetError> {
    if let ValueSchema::Option(ret) = self {
      Ok(*ret)
    } else {
      Err(ParquetError::General(String::from("")))
    }
  }
}

impl Downcast<ValueSchema> for ValueSchema {
  fn downcast(self) -> Result<ValueSchema, ParquetError> { Ok(self) }
}
impl Downcast<BoolSchema> for ValueSchema {
  fn downcast(self) -> Result<BoolSchema, ParquetError> { self.as_bool() }
}
impl Downcast<U8Schema> for ValueSchema {
  fn downcast(self) -> Result<U8Schema, ParquetError> { self.as_u8() }
}
impl Downcast<I8Schema> for ValueSchema {
  fn downcast(self) -> Result<I8Schema, ParquetError> { self.as_i8() }
}
impl Downcast<U16Schema> for ValueSchema {
  fn downcast(self) -> Result<U16Schema, ParquetError> { self.as_u16() }
}
impl Downcast<I16Schema> for ValueSchema {
  fn downcast(self) -> Result<I16Schema, ParquetError> { self.as_i16() }
}
impl Downcast<U32Schema> for ValueSchema {
  fn downcast(self) -> Result<U32Schema, ParquetError> { self.as_u32() }
}
impl Downcast<I32Schema> for ValueSchema {
  fn downcast(self) -> Result<I32Schema, ParquetError> { self.as_i32() }
}
impl Downcast<U64Schema> for ValueSchema {
  fn downcast(self) -> Result<U64Schema, ParquetError> { self.as_u64() }
}
impl Downcast<I64Schema> for ValueSchema {
  fn downcast(self) -> Result<I64Schema, ParquetError> { self.as_i64() }
}
impl Downcast<F32Schema> for ValueSchema {
  fn downcast(self) -> Result<F32Schema, ParquetError> { self.as_f32() }
}
impl Downcast<F64Schema> for ValueSchema {
  fn downcast(self) -> Result<F64Schema, ParquetError> { self.as_f64() }
}
impl Downcast<TimestampSchema> for ValueSchema {
  fn downcast(self) -> Result<TimestampSchema, ParquetError> { self.as_timestamp() }
}
impl Downcast<VecSchema> for ValueSchema {
  fn downcast(self) -> Result<VecSchema, ParquetError> { self.as_array() }
}
impl Downcast<StringSchema> for ValueSchema {
  fn downcast(self) -> Result<StringSchema, ParquetError> { self.as_string() }
}
impl<T> Downcast<ListSchema<T>> for ValueSchema
where ValueSchema: Downcast<T>
{
  default fn downcast(self) -> Result<ListSchema<T>, ParquetError> {
    let ret = self.as_list()?;
    Ok(ListSchema(ret.0.downcast()?, ret.1))
  }
}
impl Downcast<ListSchema<ValueSchema>> for ValueSchema {
  fn downcast(self) -> Result<ListSchema<ValueSchema>, ParquetError> { self.as_list() }
}
impl<K, V> Downcast<MapSchema<K, V>> for ValueSchema
where ValueSchema: Downcast<K> + Downcast<V>
{
  default fn downcast(self) -> Result<MapSchema<K, V>, ParquetError> {
    let ret = self.as_map()?;
    Ok(MapSchema(
      ret.0.downcast()?,
      ret.1.downcast()?,
      ret.2,
      ret.3,
      ret.4,
    ))
  }
}
impl Downcast<MapSchema<ValueSchema, ValueSchema>> for ValueSchema {
  fn downcast(self) -> Result<MapSchema<ValueSchema, ValueSchema>, ParquetError> {
    self.as_map()
  }
}
impl Downcast<GroupSchema> for ValueSchema {
  fn downcast(self) -> Result<GroupSchema, ParquetError> { self.as_group() }
}
impl<T> Downcast<OptionSchema<T>> for ValueSchema
where ValueSchema: Downcast<T>
{
  default fn downcast(self) -> Result<OptionSchema<T>, ParquetError> {
    let ret = self.as_option()?;
    ret.0.downcast().map(OptionSchema)
  }
}
impl Downcast<OptionSchema<ValueSchema>> for ValueSchema {
  fn downcast(self) -> Result<OptionSchema<ValueSchema>, ParquetError> {
    self.as_option()
  }
}

#[derive(Debug)]
pub struct GroupSchema(
  pub(super) Vec<ValueSchema>,
  pub(super) HashMap<String, usize>,
);
impl DebugType for GroupSchema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("GroupSchema") }
}

pub struct RootSchema<T, S>(pub String, pub S, pub PhantomData<fn(T)>);
impl<T, S> Debug for RootSchema<T, S>
where S: Debug
{
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    f.debug_tuple("RootSchema")
      .field(&self.0)
      .field(&self.1)
      .finish()
  }
}
impl<T, S> DebugType for RootSchema<T, S>
where S: DebugType
{
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("RootSchema") }
}

#[derive(Debug)]
pub struct VecSchema(pub(super) Option<u32>);
impl DebugType for VecSchema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("VecSchema") }
}

pub struct ArraySchema<T>(pub(super) PhantomData<fn(T)>);
impl<T> Debug for ArraySchema<T> {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    f.debug_tuple("ArraySchema").finish()
  }
}
impl<T> DebugType for ArraySchema<T> {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    f.write_str("ArraySchema<T>")
  }
}

pub struct TupleSchema<T>(pub(super) T);

#[derive(Debug)]
pub struct MapSchema<K, V>(
  pub(super) K,
  pub(super) V,
  pub(super) Option<String>,
  pub(super) Option<String>,
  pub(super) Option<String>,
);
impl<K, V> DebugType for MapSchema<K, V>
where
  K: DebugType,
  V: DebugType,
{
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("MapSchema") }
}
#[derive(Debug)]
pub struct OptionSchema<T>(pub(super) T);
impl<T> DebugType for OptionSchema<T>
where T: DebugType
{
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("OptionSchema") }
}
#[derive(Debug)]
pub struct ListSchema<T>(
  pub(super) T,
  pub(super) Option<(Option<String>, Option<String>)>,
);
impl<T> DebugType for ListSchema<T>
where T: DebugType
{
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("ListSchema") }
}
#[derive(Debug)]
pub struct BoolSchema;
impl DebugType for BoolSchema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("BoolSchema") }
}
#[derive(Debug)]
pub struct U8Schema;
impl DebugType for U8Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("U8Schema") }
}
#[derive(Debug)]
pub struct I8Schema;
impl DebugType for I8Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("I8Schema") }
}
#[derive(Debug)]
pub struct U16Schema;
impl DebugType for U16Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("U16Schema") }
}
#[derive(Debug)]
pub struct I16Schema;
impl DebugType for I16Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("I16Schema") }
}
#[derive(Debug)]
pub struct U32Schema;
impl DebugType for U32Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("U32Schema") }
}
#[derive(Debug)]
pub struct I32Schema;
impl DebugType for I32Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("I32Schema") }
}
#[derive(Debug)]
pub struct U64Schema;
impl DebugType for U64Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("U64Schema") }
}
#[derive(Debug)]
pub struct I64Schema;
impl DebugType for I64Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("I64Schema") }
}
#[derive(Debug)]
pub struct F64Schema;
impl DebugType for F64Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("F64Schema") }
}
#[derive(Debug)]
pub struct F32Schema;
impl DebugType for F32Schema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("F32Schema") }
}
#[derive(Debug)]
pub struct StringSchema;
impl DebugType for StringSchema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> { f.write_str("StringSchema") }
}
#[derive(Debug)]
pub enum TimestampSchema {
  Int96,
  Millis,
  Micros,
}
impl DebugType for TimestampSchema {
  fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    f.write_str("TimestampSchema")
  }
}

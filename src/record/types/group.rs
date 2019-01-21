use std::{
    collections::HashMap,
    fmt::{self, Debug},
    marker::PhantomData,
    ops::Index,
    rc::Rc,
    slice::SliceIndex,
    str, vec,
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    record::{
        reader::{GroupReader, RootReader},
        schemas::{GroupSchema, RootSchema, ValueSchema},
        types::{Root, Value},
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

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
            "Can't parse Group {:?}",
            schema
        )))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
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
                let ret = Value::reader(
                    field,
                    path,
                    curr_def_level,
                    curr_rep_level,
                    paths,
                    batch_size,
                );
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
            "Can't parse Group {:?}",
            schema
        )))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        RootReader(Group::reader(
            &schema.1,
            path,
            curr_def_level,
            curr_rep_level,
            paths,
            batch_size,
        ))
    }
}

impl Group {
    pub fn get(&self, k: &str) -> Option<&Value> {
        self.1.get(k).map(|&offset| &self.0[offset])
    }
}
impl<I> Index<I> for Group
where
    I: SliceIndex<[Value]>,
{
    type Output = <I as SliceIndex<[Value]>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.0.index(index)
    }
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

use super::*;
use arrow_array::{Array, Int64Array};
use arrow_schema::Field;

#[test]
fn list_of_offset_overflow_is_an_error_not_a_panic() {
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let child: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let too_long = vec![usize::MAX, 1];
    let error = list_of(item, &too_long, child).unwrap_err();
    assert_eq!(error.status, Status::Internal);
}

/// The shape lookups the metadata builders navigate `adbc_core`'s result schemas with. They
/// cannot fail against those constants, but this crate is loaded as a cdylib, so a mismatch must
/// come back as an `Internal` error naming what was expected — never a panic unwinding into C.
#[test]
fn shape_lookups_find_their_target_or_name_what_was_missing() {
    let item = Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
        true,
    ));
    let fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("items", DataType::List(item), true),
    ]);

    let items = field(&fields, "items").unwrap();
    assert_eq!(items.name(), "items");
    let error = field(&fields, "absent").unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("`absent` field"), "{error}");

    let item = list_item(&items).unwrap();
    assert_eq!(item.name(), "item");
    let inner = struct_fields(&item).unwrap();
    assert_eq!(inner.len(), 1);
    assert_eq!(inner[0].name(), "x");

    // A field of the wrong kind names itself, so the message says which lookup failed.
    let name = field(&fields, "name").unwrap();
    let error = list_item(&name).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("`name` to be a list"), "{error}");
    let error = struct_fields(&name).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("`name` to be a struct"), "{error}");
}

/// `list_of_nullable` is how a metadata builder says "this list is SQL NULL" rather than "this
/// list is empty" — the distinction `get_objects` needs for, say, a primary key's absent
/// `constraint_column_usage`. A null entry contributes no elements, so the surrounding entries
/// must keep their own slices.
#[test]
fn list_of_nullable_nulls_an_entry_without_shifting_its_neighbours() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let child: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let lengths = [2, 0, 1];

    let nulls = NullBuffer::from(vec![true, false, true]);
    let list = list_of_nullable(item.clone(), &lengths, child.clone(), Some(nulls)).unwrap();
    let list = list.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 3);
    assert!(list.is_valid(0));
    assert!(list.is_null(1), "the masked entry must be NULL, not []");
    assert!(list.is_valid(2));
    let values = |row: usize| {
        let slice = list.value(row);
        slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    };
    assert_eq!(values(0), vec![1, 2]);
    assert_eq!(
        values(2),
        vec![3],
        "the entry after the NULL kept its slice"
    );

    // Without a mask every entry is non-null, so a zero length is an empty list rather than NULL.
    let list = list_of(item, &lengths, child).unwrap();
    let list = list.as_any().downcast_ref::<ListArray>().unwrap();
    assert!((0..3).all(|row| list.is_valid(row)));
    assert_eq!(list.value(1).len(), 0);
}

/// A dense union must match its schema's branches exactly, even though the driver only ever
/// populates one of them: the unused branches are filled with an *empty array of the right type*
/// so the union's own type still equals the declared one.
#[test]
fn dense_union_fills_every_unused_branch_with_an_empty_array_of_its_type() {
    let fields = UnionFields::try_new(
        vec![0_i8, 1, 2],
        vec![
            Field::new("int64", DataType::Int64, true),
            Field::new("string", DataType::Utf8, true),
            Field::new("binary", DataType::Binary, true),
        ],
    )
    .unwrap();
    let populated: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
    let union = dense_union(&fields, &[(0, populated)], vec![0, 0], vec![0, 1]).unwrap();
    let union = union.as_any().downcast_ref::<UnionArray>().unwrap();

    assert_eq!(union.len(), 2);
    let value = |row: usize| {
        let slice = union.value(row);
        slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(union.type_id(0), 0);
    assert_eq!(value(0), 7);
    assert_eq!(value(1), 8);

    for (type_id, data_type) in [(1_i8, DataType::Utf8), (2, DataType::Binary)] {
        let child = union.child(type_id);
        assert_eq!(child.data_type(), &data_type, "branch {type_id}");
        assert_eq!(child.len(), 0, "branch {type_id} must be empty, not padded");
    }

    // A malformed union (an offset past the end of its branch) is an `Internal` error rather than
    // a panic, like every other failure here.
    let populated: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let error = dense_union(&fields, &[(0, populated)], vec![0, 0], vec![0, 1]).unwrap_err();
    assert_eq!(error.status, Status::Internal);
}

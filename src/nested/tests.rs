use super::*;
use arrow_array::Int64Array;
use arrow_schema::Field;

#[test]
fn list_of_offset_overflow_is_an_error_not_a_panic() {
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let child: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let too_long = vec![usize::MAX, 1];
    let error = list_of(item, &too_long, child).unwrap_err();
    assert_eq!(error.status, Status::Internal);
}

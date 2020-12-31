use crate::execute::Session;
use ast::*;
use kernel::*;

pub fn all(predicates: &Vec<Scalar>, input: &RecordBatch, state: &mut Session) -> BoolArray {
    let mut mask = BoolArray::trues(input.len());
    for p in predicates {
        mask = eval(p, &input, state).as_bool().unwrap().and(&mask);
    }
    mask
}

pub fn eval(scalar: &Scalar, input: &RecordBatch, state: &mut Session) -> Array {
    match scalar {
        Scalar::Literal(value) => value.repeat(input.len()),
        Scalar::Column(column) => {
            let find = column.canonical_name();
            input.find(&find).expect(&find).clone()
        }
        Scalar::Parameter(name, _) => {
            let value: &Array = state.variables.get(name).as_ref().expect(name);
            assert_eq!(value.len(), 1, "@{} has length {}", name, value.len());
            value.repeat(input.len())
        }
        Scalar::Call(function) => match function.as_ref() {
            Function::CurrentDate | Function::CurrentTimestamp => panic!(
                "Non-deterministic functions should have been eliminated in the rewrite phase"
            ),
            Function::IsNull(argument) => Array::Bool(eval(argument, input, state).is_null()),
            Function::Not(argument) => {
                Array::Bool(eval(argument, input, state).as_bool().unwrap().not())
            }
            Function::UnaryMinus(argument, DataType::I64) => {
                Array::I64(eval(argument, input, state).as_i64().unwrap().minus())
            }
            Function::UnaryMinus(argument, DataType::F64) => {
                Array::F64(eval(argument, input, state).as_f64().unwrap().minus())
            }
            Function::UnaryMinus(_, other) => panic!("-{} is not defined", other),
            Function::And(left, right) => {
                let left = eval(left, input, state).as_bool().unwrap();
                let right = eval(right, input, state).as_bool().unwrap();
                Array::Bool(left.and(&right))
            }
            Function::Or(left, right) => {
                let left = eval(left, input, state).as_bool().unwrap();
                let right = eval(right, input, state).as_bool().unwrap();
                Array::Bool(left.or(&right))
            }
            Function::Is(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.is(&right))
            }
            Function::Equal(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.equal(&right))
            }
            Function::NotEqual(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.not_equal(&right))
            }
            Function::Greater(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.greater(&right))
            }
            Function::GreaterOrEqual(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.greater_equal(&right))
            }
            Function::Less(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.less(&right))
            }
            Function::LessOrEqual(left, right) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                Array::Bool(left.less_equal(&right))
            }
            Function::Like(left, right) => {
                let left = eval(left, input, state).as_string().unwrap();
                let right = eval(right, input, state).as_string().unwrap();
                Array::Bool(left.like(&right))
            }
            Function::Add(left, right, DataType::I64) => {
                let left = eval(left, input, state).as_i64().unwrap();
                let right = eval(right, input, state).as_i64().unwrap();
                Array::I64(left.add(&right))
            }
            Function::Subtract(left, right, DataType::I64) => {
                let left = eval(left, input, state).as_i64().unwrap();
                let right = eval(right, input, state).as_i64().unwrap();
                Array::I64(left.subtract(&right))
            }
            Function::Multiply(left, right, DataType::I64) => {
                let left = eval(left, input, state).as_i64().unwrap();
                let right = eval(right, input, state).as_i64().unwrap();
                Array::I64(left.multiply(&right))
            }
            Function::Divide(left, right, DataType::I64) => {
                let left = eval(left, input, state).as_i64().unwrap();
                let right = eval(right, input, state).as_i64().unwrap();
                Array::I64(left.divide(&right))
            }
            Function::Add(left, right, DataType::F64) => {
                let left = eval(left, input, state).as_f64().unwrap();
                let right = eval(right, input, state).as_f64().unwrap();
                Array::F64(left.add(&right))
            }
            Function::Subtract(left, right, DataType::F64) => {
                let left = eval(left, input, state).as_f64().unwrap();
                let right = eval(right, input, state).as_f64().unwrap();
                Array::F64(left.subtract(&right))
            }
            Function::Multiply(left, right, DataType::F64) => {
                let left = eval(left, input, state).as_f64().unwrap();
                let right = eval(right, input, state).as_f64().unwrap();
                Array::F64(left.multiply(&right))
            }
            Function::Divide(left, right, DataType::F64) => {
                let left = eval(left, input, state).as_f64().unwrap();
                let right = eval(right, input, state).as_f64().unwrap();
                Array::F64(left.divide(&right))
            }
            Function::Add(_, _, other)
            | Function::Subtract(_, _, other)
            | Function::Multiply(_, _, other)
            | Function::Divide(_, _, other) => {
                panic!("Elementary math is not defined for {:?}", other)
            }
            Function::Coalesce(left, right, _) => {
                let left = eval(left, input, state);
                let right = eval(right, input, state);
                left.coalesce(&right)
            }
            Function::CaseNoValue(test, if_true, if_false, _) => {
                let test = eval(test, input, state);
                let test = test.as_bool().unwrap();
                let if_true = eval(if_true, input, state);
                let if_false = eval(if_false, input, state);
                test.blend(&if_true, &if_false)
            }
            Function::NextVal(sequence) => {
                let input = eval(sequence, input, state);
                let input = input.as_i64().unwrap();
                let mut output = I64Array::with_capacity(input.len());
                for i in 0..input.len() {
                    let next = state.storage.next_val(input.get(i).unwrap());
                    output.push(Some(next));
                }
                Array::I64(output)
            }
            Function::Xid => Array::I64(I64Array::from(vec![state.txn].repeat(input.len()))),
        },
        Scalar::Cast(scalar, data_type) => eval(scalar, input, state).cast(*data_type),
    }
}

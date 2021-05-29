#[macro_export]
macro_rules! array_comparison_operator {
    ($self:ident, $other:ident, $left:ident, $right:ident, $op:expr) => {{
        assert_eq!($self.len(), $other.len());

        let mut result = BoolArray::with_capacity($self.len());
        for i in 0..$self.len() {
            result.push(match ($self.get(i), $other.get(i)) {
                (Some($left), Some($right)) => Some($op),
                (_, _) => None,
            });
        }
        result
    }};
}

#[macro_export]
macro_rules! scalar_comparison_operator {
    ($self:ident, $other:ident, $left:ident, $right:ident, $op:expr) => {{
        let mut result = BoolArray::with_capacity($self.len());
        for i in 0..$self.len() {
            result.push(match ($self.get(i), $other) {
                (Some($left), Some($right)) => Some($op),
                (_, _) => None,
            });
        }
        result
    }};
}

#[macro_export]
macro_rules! array_unary_operator {
    ($self:ident, $value:ident, $op:expr) => {{
        let mut result = Self::with_capacity($self.len());
        for i in 0..$self.len() {
            result.push(match $self.get(i) {
                Some($value) => Some($op),
                None => None,
            });
        }
        result
    }};
}

#[macro_export]
macro_rules! array_binary_operator {
    ($self:ident, $other:ident, $left:ident, $right:ident, $op:expr) => {{
        assert_eq!($self.len(), $other.len());

        let mut result = Self::with_capacity($self.len());
        for i in 0..$self.len() {
            result.push(match ($self.get(i), $other.get(i)) {
                (Some($left), Some($right)) => Some($op),
                (_, _) => None,
            });
        }
        result
    }};
}

macro_rules! cast_operator {
    ($self:ident, $value:ident, $cast:expr, $T:ident) => {{
        let mut result = $T::with_capacity($self.len());
        for i in 0..$self.len() {
            match $self.get(i) {
                Some($value) => result.push(Some($cast)),
                None => result.push(None),
            }
        }
        result
    }};
}

macro_rules! cast_from_string {
    ($self:ident, $value:ident, $cast:expr, $T:ident) => {{
        let mut result = $T::with_capacity($self.len());
        for i in 0..$self.len() {
            match $self.get_str(i) {
                Some($value) => result.push(Some($cast)),
                None => result.push(None),
            }
        }
        result
    }};
}

macro_rules! cast_to_string {
    ($self:ident, $value:ident, $cast:expr, $T:ident) => {{
        let mut result = $T::with_capacity($self.len());
        for i in 0..$self.len() {
            match $self.get(i) {
                Some($value) => result.push_str(Some($cast)),
                None => result.push_str(None),
            }
        }
        result
    }};
}

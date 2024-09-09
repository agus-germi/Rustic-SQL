/// Este módulo define operadores para comparar tipos `Value` y aplica la lógica
/// de comparación correspondiente basada en el operador proporcionado.
use crate::extras::{get_int_value, get_str_value, Value};

/// El trait `Operations` se utiliza para definir una interfaz común para
/// diferentes operadores de comparación. Cada operador implementará este rasgo
/// para aplicar su lógica a dos argumentos de tipo `Value`.
trait Operations {
    /// Aplica la operación sobre dos argumentos de tipo `Value` y devuelve un resultado booleano.
    ///
    /// # Argumentos
    /// * `value1` - El primer `Value` a comparar.
    /// * `value2` - El segundo `Value` a comparar.
    ///
    /// # Retorna
    /// * `true` si la operación tiene éxito (es decir, si la comparación es verdadera).
    /// * `false` en caso contrario.
    fn apply_operation(&self, value1: Value, value2: Value) -> bool;
}
/// `EqualOperator` es una implementación del trait `Operations` que verifica
/// si dos valores son iguales.
struct EqualOperator;

impl Operations for EqualOperator {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool {
        let int_value1 = get_int_value(&value1);
        let int_value2 = get_int_value(&value2);
        let str_value1 = get_str_value(&value1);
        let str_value2 = get_str_value(&value2);
        match (int_value1, int_value2, str_value1, str_value2) {
            (Some(i1), Some(i2), _, _) => i1 == i2,
            (_, _, Some(s1), Some(s2)) => s1 == s2,
            _ => false,
        }
    }
}

/// `GreaterThanOperator` es una implementación del trait `Operations` que verifica
/// si el primer valor es mayor que el segundo.
struct GreaterThanOperator;

impl Operations for GreaterThanOperator {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool {
        let int_value1 = get_int_value(&value1);
        let int_value2 = get_int_value(&value2);
        let str_value1 = get_str_value(&value1);
        let str_value2 = get_str_value(&value2);
        match (int_value1, int_value2, str_value1, str_value2) {
            (Some(i1), Some(i2), _, _) => i1 > i2,
            (_, _, Some(_s1), Some(_s2)) => false,
            _ => false,
        }
    }
}

/// `LessThanOperator` es una implementación del trait `Operations` que verifica
/// si el primer valor es menor que el segundo.
struct LessThanOperator;

impl Operations for LessThanOperator {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool {
        let int_value1 = get_int_value(&value1);
        let int_value2 = get_int_value(&value2);
        let str_value1 = get_str_value(&value1);
        let str_value2 = get_str_value(&value2);
        match (int_value1, int_value2, str_value1, str_value2) {
            (Some(i1), Some(i2), _, _) => i1 < i2,
            (_, _, Some(_s1), Some(_s2)) => false,
            _ => false,
        }
    }
}

/// Filtra dos objetos `Value` basados en el operador proporcionado.
///
/// # Argumentos
/// * `value1` - El primer `Value` a comparar.
/// * `value2` - El segundo `Value` a comparar.
/// * `operator` - El operador de comparación, que puede ser uno de los siguientes:
///   - "=": Verifica si `value1` es igual a `value2`.
///   - ">": Verifica si `value1` es mayor que `value2`.
///   - "<": Verifica si `value1` es menor que `value2`.
///
/// # Retorna
/// * `true` si la comparación basada en el operador tiene éxito.
/// * `false` si la comparación falla o si se proporciona un operador no soportado.
///
/// # Ejemplo
/// ```rust
/// use sql::extras::Value;
/// use sql::operations::filter;
///
/// let value1 = Value::Int(10);
/// let value2 = Value::Int(20);
///
/// let resultado = filter(value1, value2, "<");
/// assert_eq!(resultado, true);
/// ```

pub fn filter(value1: Value, value2: Value, operator: &str) -> bool {
    let operator: Box<dyn Operations> = match operator {
        "=" => Box::new(EqualOperator),
        ">" => Box::new(GreaterThanOperator),
        "<" => Box::new(LessThanOperator),
        _ => return false,
    };
    operator.apply_operation(value1, value2)
}

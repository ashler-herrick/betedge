import pyarrow as pa


def python_to_arrow_type(python_type) -> pa.DataType:
    """
    Map Python types to PyArrow types for schema generation.

    Args:
        python_type: Python type annotation

    Returns:
        Corresponding PyArrow DataType
    """
    type_mapping = {
        int: pa.uint32(),
        float: pa.float32(),
        str: pa.string(),
    }
    return type_mapping.get(python_type, pa.string())


def generate_schema_from_model(model_class) -> dict:
    """
    Generate Arrow schema configuration from Pydantic model.

    Args:
        model_class: Pydantic model class (QuoteTick or OHLCTick)

    Returns:
        Dictionary with field_names, arrow_types, and field_count
    """
    fields = model_class.model_fields
    field_names = list(fields.keys())
    arrow_types = [python_to_arrow_type(field.annotation) for field in fields.values()]

    return {"field_names": field_names, "arrow_types": arrow_types, "field_count": len(field_names)}

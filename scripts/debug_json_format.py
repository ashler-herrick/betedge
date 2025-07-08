#!/usr/bin/env python3
"""
Debug script to compare JSON formats between expected Rust parser input
and actual Pydantic model output.

This helps identify why fast_parser is returning a list instead of bytes.
"""

import json
import orjson
from decimal import Decimal
from pprint import pprint

from betedge_data.common.models import OptionThetaDataResponse, Contract, OptionResponseItem
from betedge_data.historical.option.models import HistOptionBulkRequest


def _decimal_default(obj):
    """Default function for orjson to handle Decimal objects."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def get_expected_rust_format():
    """Get the expected JSON format from Rust test data."""
    return {
        "header": {
            "latency_ms": 100,
            "format": [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
                "date",
            ],
        },
        "response": [
            {
                "ticks": [
                    [34200000, 10, 47, 149.50, 50, 10, 47, 150.00, 50, 20231117],
                    [34260000, 15, 47, 149.75, 50, 12, 47, 150.25, 50, 20231117],
                ],
                "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1500000, "right": "C"},
            },
            {
                "ticks": [[34200000, 8, 47, 164.50, 50, 8, 47, 165.00, 50, 20231117]],
                "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1650000, "right": "C"},
            },
        ],
    }


def create_pydantic_model_equivalent():
    """Create the equivalent data using Pydantic models."""
    from betedge_data.common.models import Header

    # Create header
    header = Header(
        latency_ms=100,
        format=[
            "ms_of_day",
            "bid_size",
            "bid_exchange",
            "bid",
            "bid_condition",
            "ask_size",
            "ask_exchange",
            "ask",
            "ask_condition",
            "date",
        ],
    )

    # Create contracts
    contract1 = Contract(root="AAPL", expiration=20231117, strike=1500000, right="C")

    contract2 = Contract(root="AAPL", expiration=20231117, strike=1650000, right="C")

    # Create option response items
    item1 = OptionResponseItem(
        ticks=[
            [34200000, 10, 47, 149.50, 50, 10, 47, 150.00, 50, 20231117],
            [34260000, 15, 47, 149.75, 50, 12, 47, 150.25, 50, 20231117],
        ],
        contract=contract1,
    )

    item2 = OptionResponseItem(ticks=[[34200000, 8, 47, 164.50, 50, 8, 47, 165.00, 50, 20231117]], contract=contract2)

    # Create full response
    response = OptionThetaDataResponse(header=header, response=[item1, item2])

    return response


def compare_json_formats():
    """Compare the expected format with Pydantic model output."""
    print("=" * 80)
    print("JSON FORMAT COMPARISON")
    print("=" * 80)

    # Get expected format
    expected_format = get_expected_rust_format()

    # Get Pydantic model format
    pydantic_model = create_pydantic_model_equivalent()
    pydantic_dict = pydantic_model.to_rust_dict()

    # Convert to JSON strings
    expected_json = json.dumps(expected_format, indent=2)
    pydantic_json = orjson.dumps(pydantic_dict, default=_decimal_default, option=orjson.OPT_INDENT_2).decode("utf-8")

    print("EXPECTED FORMAT (from Rust test_data.rs):")
    print("-" * 50)
    print(expected_json)
    print()

    print("PYDANTIC MODEL OUTPUT (model_dump()):")
    print("-" * 50)
    print(pydantic_json)
    print()

    # Compare structures
    print("STRUCTURAL COMPARISON:")
    print("-" * 50)

    def compare_structure(expected, actual, path=""):
        """Recursively compare structures."""
        issues = []

        if type(expected) != type(actual):
            issues.append(f"{path}: Type mismatch - expected {type(expected)}, got {type(actual)}")
            return issues

        if isinstance(expected, dict):
            # Check keys
            expected_keys = set(expected.keys())
            actual_keys = set(actual.keys())

            missing_keys = expected_keys - actual_keys
            extra_keys = actual_keys - expected_keys

            if missing_keys:
                issues.append(f"{path}: Missing keys - {missing_keys}")
            if extra_keys:
                issues.append(f"{path}: Extra keys - {extra_keys}")

            # Recursively check common keys
            for key in expected_keys & actual_keys:
                issues.extend(compare_structure(expected[key], actual[key], f"{path}.{key}"))

        elif isinstance(expected, list):
            if len(expected) != len(actual):
                issues.append(f"{path}: Length mismatch - expected {len(expected)}, got {len(actual)}")
            else:
                for i, (exp_item, act_item) in enumerate(zip(expected, actual)):
                    issues.extend(compare_structure(exp_item, act_item, f"{path}[{i}]"))
        else:
            # Compare values for primitives
            if expected != actual:
                issues.append(f"{path}: Value mismatch - expected {expected}, got {actual}")

        return issues

    issues = compare_structure(expected_format, pydantic_dict)

    if issues:
        print("ISSUES FOUND:")
        for issue in issues:
            print(f"  ❌ {issue}")
    else:
        print("✅ Structures match perfectly!")

    print()

    # Test JSON equality
    print("JSON EQUALITY TEST:")
    print("-" * 50)

    # Parse both back to dict and compare
    expected_parsed = json.loads(expected_json)
    pydantic_parsed = json.loads(pydantic_json)

    if expected_parsed == pydantic_parsed:
        print("✅ JSON content is identical!")
    else:
        print("❌ JSON content differs!")

        # Find specific differences
        def find_differences(exp, act, path=""):
            diffs = []
            if exp != act:
                if isinstance(exp, dict) and isinstance(act, dict):
                    for key in set(exp.keys()) | set(act.keys()):
                        if key not in exp:
                            diffs.append(f"{path}.{key}: Missing in expected")
                        elif key not in act:
                            diffs.append(f"{path}.{key}: Missing in actual")
                        else:
                            diffs.extend(find_differences(exp[key], act[key], f"{path}.{key}"))
                elif isinstance(exp, list) and isinstance(act, list):
                    for i in range(max(len(exp), len(act))):
                        if i >= len(exp):
                            diffs.append(f"{path}[{i}]: Extra item in actual")
                        elif i >= len(act):
                            diffs.append(f"{path}[{i}]: Missing item in actual")
                        else:
                            diffs.extend(find_differences(exp[i], act[i], f"{path}[{i}]"))
                else:
                    diffs.append(f"{path}: {exp} != {act}")
            return diffs

        differences = find_differences(expected_parsed, pydantic_parsed)
        for diff in differences[:10]:  # Show first 10 differences
            print(f"  ❌ {diff}")
        if len(differences) > 10:
            print(f"  ... and {len(differences) - 10} more differences")


def test_fast_parser_input():
    """Test what happens when we pass the Pydantic JSON to fast_parser (if available)."""
    print("=" * 80)
    print("FAST_PARSER TEST")
    print("=" * 80)

    try:
        import fast_parser

        # Create test data
        pydantic_model = create_pydantic_model_equivalent()
        pydantic_json = orjson.dumps(pydantic_model.to_rust_dict(), default=_decimal_default).decode("utf-8")

        # Create mock stock data (with latency_ms field)
        stock_json = json.dumps(
            {
                "header": {
                    "latency_ms": 50,
                    "format": [
                        "ms_of_day",
                        "bid_size",
                        "bid_exchange",
                        "bid",
                        "bid_condition",
                        "ask_size",
                        "ask_exchange",
                        "ask",
                        "ask_condition",
                        "date",
                    ],
                },
                "response": [[34200000, 100, 47, 149.75, 50, 100, 47, 149.85, 50, 20231117]],
            }
        )

        print("Testing with Pydantic JSON input...")
        print(f"Option JSON length: {len(pydantic_json)}")
        print(f"Stock JSON length: {len(stock_json)}")

        # Try to call fast_parser
        try:
            result = fast_parser.filter_contracts_to_parquet_bytes(pydantic_json, stock_json, 20231117, 30, 0.1)
            print(
                f"✅ fast_parser returned: {type(result)} (length: {len(result) if hasattr(result, '__len__') else 'N/A'})"
            )

            if isinstance(result, list):
                print(f"❌ ERROR: fast_parser returned list instead of bytes!")
                print(f"   First few items: {result[:3] if len(result) > 0 else 'empty'}")
            else:
                print(f"✅ Success: fast_parser returned bytes as expected")

        except Exception as e:
            print(f"❌ fast_parser error: {e}")

    except ImportError:
        print("❌ fast_parser module not available")


def main():
    """Main function to run all comparisons."""
    compare_json_formats()
    test_fast_parser_input()

    print("=" * 80)
    print("RECOMMENDATIONS:")
    print("=" * 80)
    print("1. If structures match but fast_parser returns list:")
    print("   → Check Rust parser implementation for edge cases")
    print("2. If structures don't match:")
    print("   → Fix Pydantic model serialization to match expected format")
    print("3. If JSON content differs in values:")
    print("   → Check data type conversions (especially decimals/floats)")


if __name__ == "__main__":
    main()

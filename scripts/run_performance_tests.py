#!/usr/bin/env python3
"""
Quick runner for performance tests.
"""

import sys
from tests.test_performance import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPerformance tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Performance tests failed: {e}")
        sys.exit(1)

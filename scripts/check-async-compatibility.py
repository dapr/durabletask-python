#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Pre-commit hook script to check AsyncWorkflowContext compatibility.

This script can be used as a git pre-commit hook to ensure that changes
to AsyncWorkflowContext don't break compatibility with OrchestrationContext.

Usage:
    # As a pre-commit hook
    cp scripts/check-async-compatibility.py .git/hooks/pre-commit
    chmod +x .git/hooks/pre-commit
    
    # Manual execution
    python scripts/check-async-compatibility.py
"""

import subprocess
import sys
from pathlib import Path


def run_compatibility_tests():
    """Run the compatibility test suite."""
    print("🔍 Checking AsyncWorkflowContext compatibility...")
    
    # Run the CI compatibility tests
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        "tests/aio/test_ci_compatibility.py",
        "-v", "--tb=short"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Compatibility tests passed!")
        return True
    else:
        print("❌ Compatibility tests failed!")
        print("\nTest output:")
        print(result.stdout)
        if result.stderr:
            print("\nErrors:")
            print(result.stderr)
        return False


def run_compatibility_check():
    """Run the programmatic compatibility check."""
    print("🔍 Running programmatic compatibility check...")
    
    try:
        # Add current directory to path for imports
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path.cwd()))
        
        # Import and run the compatibility check
        from tests.aio.compatibility_utils import check_async_context_compatibility
        
        report = check_async_context_compatibility()
        
        if report['is_compatible']:
            print("✅ AsyncWorkflowContext is fully compatible!")
            print(f"   Enhanced methods: {len(report['extra_members'])}")
            return True
        else:
            print("❌ Compatibility issues detected!")
            print(f"   Missing members: {report['missing_members']}")
            return False
            
    except Exception as e:
        print(f"❌ Error during compatibility check: {e}")
        return False


def main():
    """Main function for the compatibility check."""
    print("🚀 AsyncWorkflowContext Compatibility Check")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("durabletask/asyncio").exists():
        print("❌ Error: Must be run from the durabletask-python root directory")
        return 1
    
    # Run both checks
    tests_passed = run_compatibility_tests()
    check_passed = run_compatibility_check()
    
    print("\n" + "=" * 50)
    
    if tests_passed and check_passed:
        print("🎉 All compatibility checks passed!")
        print("   Safe to commit changes.")
        return 0
    else:
        print("💥 Compatibility checks failed!")
        print("   Please fix compatibility issues before committing.")
        print("\nTips:")
        print("   - Ensure all OrchestrationContext properties/methods are implemented")
        print("   - Check that new methods are additive, not breaking")
        print("   - Run: python -m pytest tests/aio/test_context_compatibility.py")
        return 1


if __name__ == "__main__":
    sys.exit(main())

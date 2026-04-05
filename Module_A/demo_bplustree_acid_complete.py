#!/usr/bin/env python3
"""
B+ TREE ACID PROPERTIES - COMPLETE DEMONSTRATION
================================================

Master script that runs all four ACID property demonstrations for B+ Tree:
1. Atomicity - All-or-nothing execution
2. Consistency - Valid states only
3. Isolation - Transaction independence
4. Durability - Crash survival

This is the main entry point for demonstrating that the B+ Tree
from Assignment 2 satisfies all ACID properties in Assignment 3.
"""

import sys
import os
import subprocess
from datetime import datetime
import time

def print_header(title, width=80):
    """Print formatted header."""
    print("\n" + "=" * width)
    print(title.center(width))
    print("=" * width + "\n")

def run_demo(script_name, property_name):
    """Run a single ACID property demonstration."""
    print_header(f"RUNNING: {property_name} DEMONSTRATION")
    
    print(f"📝 Script: {script_name}")
    print(f"🎯 Testing: {property_name} property of B+ Tree")
    print(f"⏰ Start Time: {datetime.now().strftime('%H:%M:%S')}\n")
    
    try:
        # Run the demo script
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=False,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        if result.returncode == 0:
            print(f"\n✅ {property_name} demonstration PASSED\n")
            return True
        else:
            print(f"\n❌ {property_name} demonstration FAILED (exit code: {result.returncode})\n")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n❌ {property_name} demonstration TIMEOUT\n")
        return False
    except Exception as e:
        print(f"\n❌ {property_name} demonstration ERROR: {e}\n")
        return False

def main():
    """Run all B+ Tree ACID demonstrations."""
    
    print_header("B+ TREE ACID PROPERTIES - COMPLETE DEMONSTRATION", 80)
    
    print("Assignment 3 - Module A")
    print("Demonstrating that B+ Tree satisfies all ACID properties")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nThis demonstration will run 4 comprehensive tests:")
    print("  1. ATOMICITY - All-or-nothing execution")
    print("  2. CONSISTENCY - Valid states only")
    print("  3. ISOLATION - Transaction independence")
    print("  4. DURABILITY - Crash survival")
    
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define demonstrations
    demos = [
        ("demo_bplustree_atomicity.py", "ATOMICITY"),
        ("demo_bplustree_consistency.py", "CONSISTENCY"),
        ("demo_bplustree_isolation.py", "ISOLATION"),
        ("demo_bplustree_durability.py", "DURABILITY"),
    ]
    
    # Check if all demo scripts exist
    print("\n📋 PRE-FLIGHT CHECK:")
    all_exist = True
    for script, _ in demos:
        script_path = os.path.join(script_dir, script)
        if os.path.exists(script_path):
            print(f"  ✅ {script}")
        else:
            print(f"  ❌ {script} NOT FOUND")
            all_exist = False
    
    if not all_exist:
        print("\n❌ Some demonstration scripts are missing!")
        print("Please ensure all scripts are in the same directory.")
        return 1
    
    # Ask user if ready to proceed
    print("\n" + "="*80)
    response = input("Ready to run all demonstrations? (Press Enter to continue, Ctrl+C to cancel): ")
    
    # Run each demonstration
    results = []
    
    for i, (script, property_name) in enumerate(demos, 1):
        print(f"\n{'='*80}")
        print(f"DEMONSTRATION {i}/4: {property_name}")
        print(f"{'='*80}")
        
        script_path = os.path.join(script_dir, script)
        success = run_demo(script_path, property_name)
        results.append((property_name, success))
        
        # Pause between demonstrations
        if i < len(demos):
            print("\n⏸️  Pausing for 2 seconds before next demonstration...")
            time.sleep(2)
    
    # Final summary
    print_header("FINAL SUMMARY - B+ TREE ACID PROPERTIES", 80)
    
    print("Test Results:")
    all_passed = True
    for property_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {property_name:15} : {status}")
        if not success:
            all_passed = False
    
    print("\n" + "-"*80)
    
    if all_passed:
        print("\n🎉 SUCCESS: ALL ACID PROPERTIES VERIFIED FOR B+ TREE")
        print("\nConclusion:")
        print("  ✅ ATOMICITY    - All-or-nothing execution confirmed")
        print("  ✅ CONSISTENCY  - Valid states only confirmed")
        print("  ✅ ISOLATION    - Transaction independence confirmed")
        print("  ✅ DURABILITY   - Crash survival confirmed")
        print("\nThe B+ Tree from Assignment 2 Module A satisfies")
        print("all ACID properties as required by Assignment 3.")
        print("\n" + "="*80)
        return 0
    else:
        print("\n⚠️  SOME TESTS FAILED")
        print("\nPlease review the failed demonstrations above.")
        print("Common issues:")
        print("  - Missing dependencies (check requirements.txt)")
        print("  - File permission issues (WAL logs directory)")
        print("  - Database connection issues")
        print("\n" + "="*80)
        return 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n❌ Demonstration interrupted by user.")
        print("Run again when ready to complete all tests.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
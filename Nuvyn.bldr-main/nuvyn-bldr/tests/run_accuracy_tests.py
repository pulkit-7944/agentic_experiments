#!/usr/bin/env python3
"""
Accuracy Test Runner for Analyst Agent.

This script runs all accuracy tests and provides a comprehensive summary
of the Analyst Agent's performance.
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def run_all_accuracy_tests():
    """Run all accuracy tests and provide a summary."""
    print("🚀 ANALYST AGENT ACCURACY TEST SUITE")
    print("=" * 60)
    print("Running comprehensive accuracy validation...")
    print()
    
    # Import and run tests
    try:
        # Simple accuracy tests
        print("📋 Running Simple Accuracy Tests...")
        from test_analyst_agent_simple import run_simple_accuracy_tests
        simple_success = run_simple_accuracy_tests()
        print()
        
        # Business logic tests
        print("📋 Running Business Logic Tests...")
        from test_analyst_agent_business_logic import run_business_logic_tests
        business_success = run_business_logic_tests()
        print()
        
        # Comprehensive tests
        print("📋 Running Comprehensive Tests...")
        from test_analyst_agent_comprehensive import generate_comprehensive_report
        summary = generate_comprehensive_report()
        comprehensive_success = summary["average_score"] >= 80
        print()
        
        # Final summary
        print("=" * 60)
        print("🎯 FINAL ACCURACY ASSESSMENT")
        print("=" * 60)
        
        tests_passed = sum([simple_success, business_success, comprehensive_success])
        total_tests = 3
        
        print(f"✅ Simple Accuracy Tests: {'PASS' if simple_success else 'FAIL'}")
        print(f"✅ Business Logic Tests: {'PASS' if business_success else 'FAIL'}")
        print(f"✅ Comprehensive Tests: {'PASS' if comprehensive_success else 'FAIL'}")
        print()
        
        overall_score = summary["average_score"]
        overall_grade = summary["overall_grade"]
        
        print(f"📈 Overall Accuracy Score: {overall_score:.1f}/100 ({overall_grade})")
        print(f"⏱️  Average Generation Time: {summary['average_generation_time']:.2f} seconds")
        print(f"🎯 Test Success Rate: {tests_passed}/{total_tests} ({tests_passed/total_tests*100:.1f}%)")
        print()
        
        if overall_score >= 90 and tests_passed == total_tests:
            print("🎉 EXCELLENT! Analyst Agent is performing exceptionally well!")
            print("   The system is ready for production use.")
        elif overall_score >= 80 and tests_passed >= 2:
            print("👍 GOOD! Analyst Agent is performing well with minor improvements needed.")
            print("   The system is suitable for most use cases.")
        elif overall_score >= 70:
            print("⚠️  ACCEPTABLE! Analyst Agent needs some improvements.")
            print("   Review the detailed reports for specific issues.")
        else:
            print("❌ NEEDS IMPROVEMENT! Analyst Agent requires significant work.")
            print("   Focus on the failing test areas.")
        
        print()
        print("📄 Detailed reports available in:")
        print("   - output/analyst_agent_accuracy_report.json")
        print("   - output/failed_sttm_output.txt (if any failures)")
        
        return overall_score >= 80 and tests_passed >= 2
        
    except Exception as e:
        print(f"❌ Error running accuracy tests: {str(e)}")
        return False


if __name__ == "__main__":
    success = run_all_accuracy_tests()
    sys.exit(0 if success else 1) 
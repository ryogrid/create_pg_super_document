#!/usr/bin/env python3
"""
Documentation Quality Validator
Assesses the quality of generated documentation against defined criteria
"""

import json
import duckdb
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import re

def assess_documentation_quality(content: str, symbol_name: str) -> Dict[str, any]:
    """Assess the quality of a single documentation file"""
    lines = content.split('\n')
    
    quality_metrics = {
        'symbol_name': symbol_name,
        'total_length': len(content),
        'line_count': len(lines),
        'sections_found': [],
        'quality_scores': {},
        'issues': []
    }
    
    # Check for required sections
    required_sections = ['Overview', 'Definition', 'Detailed Description', 
                        'Parameters', 'Dependencies', 'Notes']
    
    for section in required_sections:
        if f'## {section}' in content:
            quality_metrics['sections_found'].append(section)
    
    # Assess Overview quality
    overview_score = assess_overview_quality(content)
    quality_metrics['quality_scores']['overview'] = overview_score
    
    # Assess Description quality  
    description_score = assess_description_quality(content)
    quality_metrics['quality_scores']['description'] = description_score
    
    # Assess Parameters quality
    parameters_score = assess_parameters_quality(content)
    quality_metrics['quality_scores']['parameters'] = parameters_score
    
    # Assess Dependencies quality
    dependencies_score = assess_dependencies_quality(content)
    quality_metrics['quality_scores']['dependencies'] = dependencies_score
    
    # Calculate overall quality score
    scores = list(quality_metrics['quality_scores'].values())
    overall_score = sum(scores) / len(scores) if scores else 0
    quality_metrics['overall_score'] = overall_score
    
    # Generate quality assessment
    if overall_score >= 8.0:
        quality_metrics['quality_level'] = 'HIGH'
    elif overall_score >= 6.0:
        quality_metrics['quality_level'] = 'MEDIUM'
    else:
        quality_metrics['quality_level'] = 'LOW'
        quality_metrics['issues'].append('Overall documentation quality below acceptable threshold')
    
    return quality_metrics

def assess_overview_quality(content: str) -> float:
    """Assess the quality of the Overview section"""
    overview_match = re.search(r'## Overview\s*\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
    if not overview_match:
        return 0.0
    
    overview_text = overview_match.group(1).strip()
    if not overview_text:
        return 1.0
    
    # Remove markdown formatting for sentence counting
    clean_text = re.sub(r'[`*_]', '', overview_text)
    sentences = [s.strip() for s in clean_text.split('.') if s.strip()]
    
    score = 2.0  # Base score
    
    # Length criteria
    if len(overview_text) >= 150:
        score += 2.0
    elif len(overview_text) >= 100:
        score += 1.0
    
    # Sentence count criteria
    if len(sentences) >= 3:
        score += 3.0
    elif len(sentences) >= 2:
        score += 2.0
    elif len(sentences) >= 1:
        score += 1.0
    
    # Content quality indicators
    quality_indicators = ['PostgreSQL', 'function', 'table', 'transaction', 'buffer', 'page', 'tuple']
    found_indicators = sum(1 for indicator in quality_indicators if indicator.lower() in overview_text.lower())
    score += min(found_indicators * 0.5, 3.0)
    
    return min(score, 10.0)

def assess_description_quality(content: str) -> float:
    """Assess the quality of the Detailed Description section"""
    desc_match = re.search(r'## Detailed Description\s*\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
    if not desc_match:
        return 0.0
    
    desc_text = desc_match.group(1).strip()
    if not desc_text:
        return 1.0
    
    clean_text = re.sub(r'[`*_]', '', desc_text)
    sentences = [s.strip() for s in clean_text.split('.') if s.strip()]
    
    score = 2.0  # Base score
    
    # Length criteria
    if len(desc_text) >= 300:
        score += 3.0
    elif len(desc_text) >= 200:
        score += 2.0
    elif len(desc_text) >= 100:
        score += 1.0
    
    # Sentence count criteria  
    if len(sentences) >= 4:
        score += 3.0
    elif len(sentences) >= 3:
        score += 2.0
    elif len(sentences) >= 2:
        score += 1.0
    
    # Technical depth indicators
    tech_terms = ['implementation', 'algorithm', 'performance', 'optimization', 'architecture', 
                 'MVCC', 'WAL', 'buffer', 'transaction', 'locking']
    found_terms = sum(1 for term in tech_terms if term.lower() in desc_text.lower())
    score += min(found_terms * 0.4, 2.0)
    
    return min(score, 10.0)

def assess_parameters_quality(content: str) -> float:
    """Assess the quality of the Parameters section"""
    params_match = re.search(r'## Parameters.*?\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
    if not params_match:
        return 5.0  # Neutral score if no parameters section (might be appropriate)
    
    params_text = params_match.group(1).strip()
    if not params_text:
        return 2.0
    
    # Count parameter entries
    param_entries = re.findall(r'- `[^`]+`:', params_text)
    
    score = 3.0  # Base score
    
    # Number of parameters documented
    if len(param_entries) >= 5:
        score += 2.0
    elif len(param_entries) >= 3:
        score += 1.5
    elif len(param_entries) >= 1:
        score += 1.0
    
    # Quality of parameter descriptions
    avg_param_length = 0
    if param_entries:
        total_desc_length = sum(len(desc) for desc in re.findall(r'- `[^`]+`: ([^\n]*)', params_text))
        avg_param_length = total_desc_length / len(param_entries) if param_entries else 0
    
    if avg_param_length >= 80:
        score += 3.0
    elif avg_param_length >= 50:
        score += 2.0
    elif avg_param_length >= 30:
        score += 1.0
    
    return min(score, 10.0)

def assess_dependencies_quality(content: str) -> float:
    """Assess the quality of the Dependencies section"""
    deps_match = re.search(r'## Dependencies\s*\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
    if not deps_match:
        return 3.0  # Low score if missing dependencies
    
    deps_text = deps_match.group(1).strip()
    if not deps_text:
        return 2.0
    
    score = 4.0  # Base score
    
    # Check for both called functions and called from sections
    has_called_functions = 'called functions' in deps_text.lower() or 'functions called' in deps_text.lower()
    has_called_from = 'called from' in deps_text.lower()
    
    if has_called_functions and has_called_from:
        score += 3.0
    elif has_called_functions or has_called_from:
        score += 2.0
    
    # Count dependency entries
    func_entries = len(re.findall(r'- `[^`]+`', deps_text))
    if func_entries >= 4:
        score += 2.0
    elif func_entries >= 2:
        score += 1.0
    
    # Check for explanations
    has_explanations = ' - ' in deps_text and len(deps_text) > 200
    if has_explanations:
        score += 1.0
    
    return min(score, 10.0)

def validate_recent_documentation(doc_db_file: str = 'data/documents.duckdb') -> Dict[str, any]:
    """Validate quality of recently generated documentation"""
    try:
        doc_con = duckdb.connect(doc_db_file)
        
        # Get recent documents (assuming they were just added)
        recent_docs = doc_con.execute("""
            SELECT symbol_name, content 
            FROM documents 
            WHERE content IS NOT NULL AND content != ''
            ORDER BY updated_at DESC 
            LIMIT 20
        """).fetchall()
        
        doc_con.close()
        
        if not recent_docs:
            return {"error": "No recent documents found for validation"}
        
        validation_results = {
            'total_documents': len(recent_docs),
            'document_assessments': [],
            'summary_stats': {
                'high_quality_count': 0,
                'medium_quality_count': 0,
                'low_quality_count': 0,
                'average_overall_score': 0.0
            }
        }
        
        total_score = 0.0
        
        for symbol_name, content in recent_docs:
            assessment = assess_documentation_quality(content, symbol_name)
            validation_results['document_assessments'].append(assessment)
            
            total_score += assessment['overall_score']
            
            if assessment['quality_level'] == 'HIGH':
                validation_results['summary_stats']['high_quality_count'] += 1
            elif assessment['quality_level'] == 'MEDIUM':
                validation_results['summary_stats']['medium_quality_count'] += 1
            else:
                validation_results['summary_stats']['low_quality_count'] += 1
        
        validation_results['summary_stats']['average_overall_score'] = total_score / len(recent_docs)
        
        return validation_results
        
    except Exception as e:
        return {"error": f"Validation failed: {e}"}

def main():
    """Main function to run documentation quality validation"""
    validation_results = validate_recent_documentation()
    
    if 'error' in validation_results:
        print(json.dumps(validation_results, indent=2))
        return
    
    # Output results
    print("=== DOCUMENTATION QUALITY VALIDATION REPORT ===")
    print(f"Total documents assessed: {validation_results['total_documents']}")
    print(f"Average overall score: {validation_results['summary_stats']['average_overall_score']:.2f}/10.0")
    print(f"High quality: {validation_results['summary_stats']['high_quality_count']}")
    print(f"Medium quality: {validation_results['summary_stats']['medium_quality_count']}")
    print(f"Low quality: {validation_results['summary_stats']['low_quality_count']}")
    
    # Show detailed results for low quality documents
    low_quality_docs = [doc for doc in validation_results['document_assessments'] 
                       if doc['quality_level'] == 'LOW']
    
    if low_quality_docs:
        print("\n=== LOW QUALITY DOCUMENTS REQUIRING ATTENTION ===")
        for doc in low_quality_docs:
            print(f"- {doc['symbol_name']}: Score {doc['overall_score']:.1f}/10.0")
            if doc['issues']:
                for issue in doc['issues']:
                    print(f"  Issue: {issue}")
    
    # Save detailed results to JSON file for automated processing
    with open('quality_report.json', 'w') as f:
        json.dump(validation_results, f, indent=2)
    
    print(f"\nDetailed report saved to: quality_report.json")

if __name__ == "__main__":
    main()
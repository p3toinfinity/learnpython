#!/usr/bin/env python3
"""
Generate PDF with Data Analytics Engineer Interview Preparation Guide
Run: python generate_interview_guide_pdf.py
"""

from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from datetime import datetime

def create_interview_guide_pdf(filename="interview_preparation_guide.pdf"):
    """Create a PDF with interview preparation guide"""
    
    # Create PDF document
    doc = SimpleDocTemplate(filename, pagesize=letter,
                           rightMargin=72, leftMargin=72,
                           topMargin=72, bottomMargin=18)
    
    # Container for the 'Flowable' objects
    story = []
    
    # Define styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1a237e'),
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#283593'),
        spaceAfter=12,
        spaceBefore=12,
        fontName='Helvetica-Bold'
    )
    
    subheading_style = ParagraphStyle(
        'CustomSubHeading',
        parent=styles['Heading3'],
        fontSize=14,
        textColor=colors.HexColor('#3949ab'),
        spaceAfter=8,
        spaceBefore=8,
        fontName='Helvetica-Bold'
    )
    
    normal_style = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=11,
        spaceAfter=6,
        alignment=TA_JUSTIFY,
        leading=14
    )
    
    bullet_style = ParagraphStyle(
        'CustomBullet',
        parent=styles['Normal'],
        fontSize=11,
        spaceAfter=4,
        leftIndent=20,
        bulletIndent=10,
        leading=14
    )
    
    # Title
    story.append(Paragraph("Data Analytics Engineer", title_style))
    story.append(Paragraph("Python Coding Interview Preparation Guide", title_style))
    story.append(Spacer(1, 0.2*inch))
    story.append(Paragraph(f"<i>Generated on: {datetime.now().strftime('%B %d, %Y')}</i>", 
                          styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    story.append(PageBreak())
    
    # Section 1: What to Expect
    story.append(Paragraph("What to Expect", heading_style))
    
    story.append(Paragraph("<b>1. Python Fundamentals</b>", subheading_style))
    items = [
        "Data structures: lists, dictionaries, sets, tuples",
        "List comprehensions and generator expressions",
        "Functions, decorators, lambda functions",
        "Object-oriented programming basics",
        "Error handling (try/except)",
        "Working with modules and packages"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>2. Data Manipulation (High Priority)</b>", subheading_style))
    items = [
        "Pandas: filtering, grouping, aggregations, merges, pivots",
        "NumPy: array operations, broadcasting",
        "Data cleaning: handling missing values, duplicates, outliers",
        "Data transformation: reshaping, pivoting, melting"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>3. Problem-Solving Approach</b>", subheading_style))
    items = [
        "Breaking down problems into smaller parts",
        "Writing clean, readable code",
        "Explaining your thought process",
        "Edge cases and error handling",
        "Code efficiency (time/space complexity)"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>4. Common Interview Patterns</b>", subheading_style))
    items = [
        "Data processing tasks (ETL-like)",
        "Aggregations and statistics",
        "Data validation and quality checks",
        "Working with nested data structures",
        "String/text processing"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(PageBreak())
    
    # Section 2: ML Engineer Perspective
    story.append(Paragraph("What an ML Engineer Will Focus On", heading_style))
    
    story.append(Paragraph("<b>Code Quality</b>", subheading_style))
    items = [
        "Clean, readable code",
        "Proper naming conventions",
        "DRY (Don't Repeat Yourself) principles",
        "Comments and docstrings",
        "Modular functions"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>Data Handling</b>", subheading_style))
    items = [
        "Efficient data processing",
        "Memory considerations",
        "Handling large datasets conceptually",
        "Data validation"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>Problem-Solving</b>", subheading_style))
    items = [
        "Logical thinking",
        "Breaking complex problems into steps",
        "Testing edge cases"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(PageBreak())
    
    # Section 3: Common Interview Question Types
    story.append(Paragraph("Common Interview Question Types", heading_style))
    
    question_types = [
        ("1. Data Transformation", "Given a list of dictionaries, group by category and calculate averages. Clean and transform a messy dataset."),
        ("2. Aggregations and Statistics", "Find the top N items by some metric. Calculate rolling averages or percentiles."),
        ("3. Data Validation", "Write a function to validate data quality. Check for missing values, duplicates, outliers."),
        ("4. String/Text Processing", "Parse and clean text data. Extract patterns from strings."),
        ("5. Algorithmic Thinking", "Find duplicates efficiently. Merge or join datasets.")
    ]
    
    for title, desc in question_types:
        story.append(Paragraph(f"<b>{title}</b>", subheading_style))
        story.append(Paragraph(f"<i>Example:</i> {desc}", normal_style))
        story.append(Spacer(1, 0.1*inch))
    
    story.append(PageBreak())
    
    # Section 4: Preparation Tips
    story.append(Paragraph("Preparation Tips", heading_style))
    
    story.append(Paragraph("<b>1. Practice Coding</b>", subheading_style))
    items = [
        "LeetCode Easy/Medium (focus on array/string problems)",
        "HackerRank Python challenges",
        "Pandas exercises (Kaggle, DataCamp)"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>2. Review These Topics</b>", subheading_style))
    items = [
        "List comprehensions",
        "Dictionary operations",
        "Pandas: groupby, merge, pivot, apply",
        "Lambda functions and map/filter/reduce",
        "Generators and iterators",
        "Error handling"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>3. Practice Explaining</b>", subheading_style))
    items = [
        "Think out loud",
        "Explain your approach before coding",
        "Discuss trade-offs",
        "Mention edge cases"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>4. Code Style</b>", subheading_style))
    items = [
        "Use meaningful variable names",
        "Write small, focused functions",
        "Add comments for complex logic",
        "Handle edge cases"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(PageBreak())
    
    # Section 5: Sample Questions
    story.append(Paragraph("Sample Questions You Might Get", heading_style))
    
    sample_questions = [
        "Given a CSV-like structure, find the average value grouped by category",
        "Write a function to clean phone numbers in various formats",
        "Find duplicate records in a dataset efficiently",
        "Transform nested JSON data into a flat structure",
        "Calculate running statistics (mean, median) on a time series"
    ]
    
    for i, question in enumerate(sample_questions, 1):
        story.append(Paragraph(f"{i}. {question}", bullet_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>What to Emphasize</b>", subheading_style))
    items = [
        "Your data engineering experience (ETL, pipelines)",
        "Understanding of data quality and validation",
        "Ability to write production-ready code",
        "Problem-solving approach",
        "Willingness to learn ML concepts if needed"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("<b>Red Flags to Avoid</b>", subheading_style))
    items = [
        "Writing code without explaining",
        "Not handling edge cases",
        "Inefficient solutions (nested loops when not needed)",
        "Poor variable naming",
        "No error handling"
    ]
    for item in items:
        story.append(Paragraph(f"• {item}", bullet_style))
    
    story.append(PageBreak())
    
    # Section 6: Day-of Tips
    story.append(Paragraph("Day-of Interview Tips", heading_style))
    
    tips = [
        "Ask clarifying questions",
        "Start with a simple solution, then optimize",
        "Test your code with examples",
        "Discuss time/space complexity if relevant",
        "Show enthusiasm for learning"
    ]
    
    for tip in tips:
        story.append(Paragraph(f"• {tip}", bullet_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Quick Review Checklist</b>", subheading_style))
    checklist = [
        "Python data structures (list, dict, set, tuple)",
        "List/dict comprehensions",
        "Pandas basics (groupby, merge, filter)",
        "Error handling (try/except)",
        "Functions and lambda functions",
        "String manipulation",
        "Working with dictionaries and nested structures"
    ]
    
    for item in checklist:
        story.append(Paragraph(f"☐ {item}", bullet_style))
    
    story.append(Spacer(1, 0.2*inch))
    story.append(Paragraph("<b>Good luck with your interview!</b>", 
                          ParagraphStyle('GoodLuck', parent=styles['Heading2'], 
                                        fontSize=16, textColor=colors.HexColor('#2e7d32'),
                                        alignment=TA_CENTER, spaceAfter=20)))
    
    # Build PDF
    doc.build(story)
    print(f"✓ PDF created successfully: {filename}")

if __name__ == "__main__":
    try:
        create_interview_guide_pdf()
    except ImportError:
        print("Error: reportlab library not found.")
        print("Please install it using: pip install reportlab")
    except Exception as e:
        print(f"Error creating PDF: {e}")


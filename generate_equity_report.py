from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

def create_institutional_report(filename):
    doc = SimpleDocTemplate(filename, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    elements = []
    styles = getSampleStyleSheet()

    # --- Header ---
    header_style = ParagraphStyle('Header', parent=styles['Normal'], fontSize=10, textColor=colors.gray)
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=20, textColor=colors.darkblue, spaceAfter=12)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=12, spaceAfter=20)

    elements.append(Paragraph("INSTITUTIONAL EQUITY RESEARCH | GLOBAL STRATEGY", header_style))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("The Venezuela Reconstruction Playbook", title_style))
    elements.append(Paragraph("<b>Date:</b> January 8, 2026", subtitle_style))
    elements.append(Spacer(1, 15))

    # --- Executive Summary ---
    elements.append(Paragraph("Executive Summary", styles['Heading2']))
    summary_text = """
    Following the January 7 announcement of the "Oil for American Goods" agreement, we have identified a high-conviction basket
    of 12 equities positioned to capture the resulting capital flows. The deal creates a closed-loop economy where Venezuelan
    oil revenue is escrowed strictly for the purchase of US-made infrastructure, agricultural, and medical products.
    """
    elements.append(Paragraph(summary_text, styles['Normal']))
    elements.append(Spacer(1, 20))

    # --- Strategic Watchlist Table ---
    elements.append(Paragraph("1. Strategic Watchlist", styles['Heading2']))

    table_data = [['Ticker', 'Company', 'Sector', 'Role', 'Catalyst']]

    stocks = [
        ['HAL', 'Halliburton', 'Energy Svc', 'Rig Repair/Production', 'New Oil Deal'],
        ['CVX', 'Chevron', 'Oil & Gas', 'Principal Partner', 'Export Logistics'],
        ['VLO', 'Valero', 'Refining', 'Heavy Crude Refining', 'Oil Deal'],
        ['GEV', 'GE Vernova', 'Power', 'Grid Turbines', 'Electric Grid'],
        ['PWR', 'Quanta Svcs', 'Infra', 'Transmission Lines', 'Electric Grid'],
        ['ETN', 'Eaton Corp', 'Electrical', 'Grid Stability', 'Electric Grid'],
        ['ADM', 'Archer-Daniels', 'Agri', 'Food Export', 'Ag Products'],
        ['ABT', 'Abbott Labs', 'Med/Nutri', 'Diagnostics/Aid', 'Med Devices'],
        ['GEHC', 'GE HealthCare', 'MedTech', 'Imaging Infra', 'Med Equipment'],
        ['SYK', 'Stryker', 'MedTech', 'Surgical Refit', 'Med Devices'],
        ['PFE', 'Pfizer', 'Pharma', 'Essential Meds', 'Medicines'],
        ['MCK', 'McKesson', 'Logistics', 'Pharma Distro', 'Medicines']
    ]
    table_data.extend(stocks)

    t = Table(table_data, colWidths=[40, 90, 70, 110, 100])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 25))

    # --- Technical Analysis Section ---
    elements.append(Paragraph("2. Technical Analysis & Entry Strategy", styles['Heading2']))

    # Style for stock blocks
    stock_header = ParagraphStyle('StockHead', parent=styles['Heading3'], fontSize=11, textColor=colors.darkblue, spaceBefore=10)
    stock_meta = ParagraphStyle('StockMeta', parent=styles['Normal'], fontSize=10, textColor=colors.black, leftIndent=10)
    stock_entry = ParagraphStyle('StockEntry', parent=styles['Normal'], fontSize=10, textColor=colors.darkgreen, fontName='Helvetica-Bold', leftIndent=10)

    # Detailed Data
    tech_data = [
        {"t": "HAL", "p": "$28.50", "struc": "Reclaiming 20-day MA. Momentum building.", "entry": "Buy $28.00 - $28.25 (Stop $27.40)"},
        {"t": "CVX", "p": "$166.25", "struc": "Testing 52-wk highs. Breakout imminent.", "entry": "Buy Breakout >$169 or Retest $162"},
        {"t": "VLO", "p": "$185.90", "struc": "All-Time Highs. Extended.", "entry": "Wait for pullback to $178.00"},
        {"t": "GEV", "p": "$662.00", "struc": "Strong uptrend channel. Recent dip.", "entry": "Accumulate $635 - $645"},
        {"t": "PWR", "p": "$439.00", "struc": "Bull flag below $450 resistance.", "entry": "Buy $425 - $430 (Stop $415)"},
        {"t": "ETN", "p": "$322.00", "struc": "Consolidating mid-range.", "entry": "Buy dip to $315"},
        {"t": "ADM", "p": "$58.20", "struc": "Bearish double-top, finding support.", "entry": "Value Buy at $57.00"},
        {"t": "ABT", "p": "$125.00", "struc": "Range-bound ($120-$130).", "entry": "Accumulate $122 - $123"},
        {"t": "GEHC", "p": "$87.50", "struc": "Bullish flag pattern.", "entry": "Buy $85.00"},
        {"t": "SYK", "p": "$367.00", "struc": "Strong trend, slightly overextended.", "entry": "Wait for 20-day MA touch at $360"},
        {"t": "PFE", "p": "$25.20", "struc": "Bottoming base. High yield.", "entry": "Value Buy at $24.80"},
        {"t": "MCK", "p": "$820.00", "struc": "Uptrend exhaustion. Divergence.", "entry": "Wait for correction to $780"},
    ]

    for item in tech_data:
        # Keep stock block together to avoid orphans
        block = [
            Paragraph(f"{item['t']} - Price: {item['p']}", stock_header),
            Paragraph(f"<b>Structure:</b> {item['struc']}", stock_meta),
            Paragraph(f"<b>Action:</b> {item['entry']}", stock_entry),
            Spacer(1, 5)
        ]
        elements.append(KeepTogether(block))

    # Disclaimer
    elements.append(Spacer(1, 30))
    disclaimer = """
    <b>Disclaimer:</b> This report is for informational purposes only. Technical levels are based on market data as of Jan 8, 2026.
    Investments carry risk.
    """
    elements.append(Paragraph(disclaimer, ParagraphStyle('Disc', parent=styles['Normal'], fontSize=8, textColor=colors.grey)))

    doc.build(elements)

create_institutional_report("Venezuela_Reconstruction_Report_Jan8_2026.pdf")

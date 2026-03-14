# LineFix PDF

LineFix PDF converts messy text-based PDFs into clean Excel-ready output.

It extracts text from text PDFs, uses Amazon Nova Lite to reconstruct logical grouped items, preserves headings and continuation lines, and exports the cleaned result to Excel.

## Built with
- Python
- Streamlit
- Amazon Bedrock
- Amazon Nova Lite
- pdfplumber
- openpyxl
- boto3
- pandas

## How to run
1. Install dependencies:
   pip install -r requirements.txt

2. Run the app:
   streamlit run app.py

## Notes
- This app is designed for text-based PDFs.
- It currently uses Amazon Bedrock / Nova Lite for structure reconstruction.
